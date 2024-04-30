"""Metadata management functions"""
import fnmatch
import logging
import math
import ntpath
from datetime import datetime

import google.api_core
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.hooks.jdbc_hook import JdbcHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.postgres_hook import PostgresHook
from dateutil import relativedelta
from google.cloud import bigquery, firestore
from numpy.core.numeric import outer


def get_dtypes(metadata_collection, source_connection, source_schema, source_table, merge_schema):
    """
    Store or retrieve dtypes from firestore for a particular database table
    """
    fs_db = firestore.Client()
    doc_ref = fs_db.collection(metadata_collection).document(f'{source_connection}.{source_schema}.{source_table}')
    dtypes_doc = doc_ref.get()
    found = False
    reference_dtypes = {}
    if dtypes_doc.exists:
        data = dtypes_doc.to_dict()
        if 'dtypes' in data:
            reference_dtypes = data['dtypes']
            found = True
            logging.info('Retrieved %s', reference_dtypes)
    if not found:
        if source_connection.startswith('oracle'):
            hook = OracleHook(source_connection)
            #pylint: disable=C0301
            sample_sql = f'''select * from {source_schema.upper()}.{source_table.upper()} where ROWNUM < 100000'''
        elif source_connection.startswith('mssql'):
            hook = MsSqlHook(mssql_conn_id=source_connection)
            sample_sql = f'''select TOP 100000 * from {source_schema.upper()}.{source_table.upper()}'''
        sample_df = hook.get_pandas_df(sample_sql)
        sample_df.columns = sample_df.columns.str.lower()
        reference_dtypes = sample_df.dtypes.apply(lambda x: x.name).to_dict()
        data = {
            u'id': f"{source_connection}.{source_schema}.{source_table}",
            u'target_schema_table': f"{merge_schema}.{source_table}",
            u'dtypes': reference_dtypes
        }
        doc_ref.set(data, merge=True)
        logging.info('Stored %s', reference_dtypes)
    return reference_dtypes

def get_merge_keys(metadata_collection, source_connection, source_schema, source_table, merge_schema):
    """
    Store or retrieve merge keys from firestore for a particular oracle source table
    """
    fs_db = firestore.Client()
    table_spec = f'{source_connection}.{source_schema}.{source_table}'
    merge_keys_doc_ref = fs_db.collection(metadata_collection).document(table_spec)
    merge_keys_doc = merge_keys_doc_ref.get()
    merge_keys = []
    found = False
    if merge_keys_doc.exists:
        data = merge_keys_doc.to_dict()
        if 'keys' in data:
            merge_keys = data['keys']
            if len(merge_keys) > 0:
                found = True
    if not found:
        if source_connection.startswith('oracle'):
            hook = OracleHook(source_connection)
            #pylint: disable=C0301
            sql = f"SELECT cols.column_name FROM all_constraints cons NATURAL JOIN all_cons_columns cols WHERE cons.constraint_type = 'P' AND owner = '{source_schema.upper()}' and table_name = '{source_table.upper()}'"
        elif source_connection.startswith('mssql'):
            hook = MsSqlHook(mssql_conn_id=source_connection)
            sql = f"""select schema_name(tab.schema_id) as [schema_name],
                    pk.[name] as pk_name,
                    ic.index_column_id as column_id,
                    col.[name] as column_name,
                    tab.[name] as table_name
                from sys.tables tab
                    inner join sys.indexes pk
                        on tab.object_id = pk.object_id
                        and pk.is_primary_key = 1
                    inner join sys.index_columns ic
                        on ic.object_id = pk.object_id
                        and ic.index_id = pk.index_id
                    inner join sys.columns col
                        on pk.object_id = col.object_id
                        and col.column_id = ic.column_id
                where
                tab.schema_id = '{source_schema}' and tab.name = '{source_table}'
                order by schema_name(tab.schema_id),
                    pk.[name],
                    ic.index_column_id"""
        else:
            logging.error("Merge keys not found for %s", table_spec)
        merge_keys_df = hook.get_pandas_df(sql)
        if len(merge_keys_df) > 0:
            merge_keys = merge_keys_df['COLUMN_NAME'].str.lower().to_list()
            data = {
                u'id': f"{source_connection}.{source_schema}.{source_table}",
                u'target_schema_table': f"{merge_schema}.{source_table}",
                u'keys' : merge_keys
            }
            merge_keys_doc_ref.set(data, merge=True)
        else:
            data = {
                u'keys' : []
            }
            merge_keys_doc_ref.set(data, merge=True)
            merge_keys = []
    return merge_keys

def map_oracle_dtypes_to_avro(source_connection, source_schema, source_table):
    """
    Generate a comma delimited list of column name and data types for the specified Oracle table
    """
    hook = OracleHook(source_connection)
    sql = f"""SELECT * FROM ALL_TAB_COLUMNS WHERE OWNER = '{source_schema.upper()}' AND TABLE_NAME = '{source_table.upper()}' ORDER BY COLUMN_ID"""
    dtypes_df = hook.get_pandas_df(sql)
    output_cols = ""
    data_type = ""
    num_rows = len(dtypes_df)
    for index, row in dtypes_df.iterrows():
        column = row['COLUMN_NAME']
        oracle_dtype = row['DATA_TYPE']
        scale = row['DATA_SCALE']
        if oracle_dtype == 'NUMBER':
            if scale > 0:
                data_type = "Double"
            else:
                if row['DATA_PRECISION'] > 9:
                    data_type = "Long"
                else:
                    data_type = "Integer"
        elif oracle_dtype == 'FLOAT':
            data_type = "Double"
        elif oracle_dtype.startswith('DATE') or oracle_dtype.startswith('TIMESTAMP'):
            data_type = "String"
        else:
            data_type = "String"
        output_cols = output_cols + f"{column.upper()}={data_type}"
        if index < (num_rows - 1):
            output_cols = output_cols + ", "
    return output_cols

def map_oracle_dtypes_to_bigquery(source_connection, source_schema, source_table):
    """
    Generate a BigQuery json schema for the specified Oracle table
    """

    hook = OracleHook(source_connection)
    sql = f"""SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, NULLABLE FROM ALL_TAB_COLUMNS WHERE OWNER = '{source_schema.upper()}' AND TABLE_NAME = '{source_table.upper()}' ORDER BY COLUMN_ID"""
    logging.info(sql)
    dtypes_df = hook.get_pandas_df(sql)
    output_cols = []
    bigquery_dtype = ""
    for index, row in dtypes_df.iterrows():
        column_name = row['COLUMN_NAME']
        oracle_dtype = row['DATA_TYPE']
        if row['NULLABLE'] == "Y":
            bigquery_mode = "NULLABLE"
        else:
            bigquery_mode = "REQUIRED"
        if oracle_dtype == 'NUMBER':
            if row['DATA_SCALE'] is None or math.isnan(row['DATA_SCALE']) or row['DATA_SCALE'] > 0:
                bigquery_dtype = "NUMERIC"
            else:
                bigquery_dtype = "INT64"
        elif oracle_dtype == 'LONG':
            bigquery_dtype = "INT64"
        elif oracle_dtype == "BLOB":
            bigquery_dtype = "BYTES"
        elif oracle_dtype == 'FLOAT':
            bigquery_dtype = "FLOAT64"
        elif oracle_dtype == 'DATE':
            bigquery_dtype = "TIMESTAMP"
        elif oracle_dtype == 'DATETIME':
            bigquery_dtype = "TIMESTAMP"
        elif oracle_dtype.startswith('TIMESTAMP'):
            bigquery_dtype = "TIMESTAMP"
        else:
            bigquery_dtype = "STRING"
        output_cols.append({ "name": column_name.lower(), "type": bigquery_dtype, "mode": bigquery_mode })
    logging.info(output_cols)
    return output_cols

def map_as400_dtypes_to_bigquery(source_connection, source_schema, source_table):
    """
    Generate a BigQuery json schema for the specified AS400 table
    """

    hook = JdbcHook(source_connection)
    sql = f"""
    SELECT COLUMN_NAME, COLUMN_TEXT, DATA_TYPE, NUMERIC_SCALE, NUMERIC_PRECISION, NUMERIC_PRECISION_RADIX, IS_NULLABLE
    FROM QSYS2.SYSCOLUMNS
    WHERE SYSTEM_TABLE_SCHEMA = '{source_schema.upper()}'
    AND SYSTEM_TABLE_NAME = '{source_table.upper()}'
    ORDER BY ORDINAL_POSITION"""
    logging.info(sql)
    dtypes_df = hook.get_pandas_df(sql)
    output_cols = []
    bigquery_dtype = ""

    for index, row in dtypes_df.iterrows():
        column_name = row['COLUMN_NAME']
        as400_dtype = row['DATA_TYPE']
        if row['IS_NULLABLE'] == "Y":
            bigquery_mode = "NULLABLE"
        else:
            bigquery_mode = "REQUIRED"
        if as400_dtype == 'DECIMAL' or as400_dtype == 'NUMERIC':
            if row['NUMERIC_SCALE'] is None or math.isnan(row['NUMERIC_SCALE']) or row['NUMERIC_SCALE'] > 0:
                bigquery_dtype = "NUMERIC"
            else:
                bigquery_dtype = "INT64"
        elif as400_dtype == "DATE":
            bigquery_dtype = "TIMESTAMP"
        elif as400_dtype == "TIMESTMP":
            bigquery_dtype = "TIMESTAMP"
        else:
            bigquery_dtype = "STRING"
        output_cols.append({ "name": column_name.lower(), "type": bigquery_dtype, "mode": bigquery_mode , "description": row['COLUMN_TEXT'] })
    logging.info(output_cols)
    return output_cols

def map_postgres_dtypes_to_bigquery(source_connection, source_schema, source_table):
    """
    Generate a BigQuery json schema for the specified Postgres table
    """
    hook = PostgresHook(source_connection)
    sql = f"""select column_name,data_type,numeric_precision,numeric_scale,is_nullable from information_schema.columns where table_schema = '{source_schema.lower()}' and table_name = '{source_table.lower()}' order by ordinal_position"""
    logging.info(sql)
    dtypes_df = hook.get_pandas_df(sql)
    output_cols = []
    bigquery_dtype = ""
    for index, row in dtypes_df.iterrows():
        column_name = row['column_name']
        postgres_dtype = row['data_type']
        if row['is_nullable'] == "YES":
            bigquery_mode = "NULLABLE"
        else:
            bigquery_mode = "REQUIRED"
        if postgres_dtype == 'numeric':
            if row['numeric_scale'] > 0 or math.isnan(row['numeric_scale']):
                bigquery_dtype = "NUMERIC"
            else:
                bigquery_dtype = "INT64"
        elif postgres_dtype == 'bigint' or postgres_dtype == 'smallint' or postgres_dtype == 'integer':
            bigquery_dtype = "INT64"
        elif postgres_dtype == "decimal":
            bigquery_dtype = "NUMERIC"
        elif postgres_dtype == "boolean":
            bigquery_dtype = "BOOLEAN"
        elif postgres_dtype == 'double precision' or postgres_dtype == "real":
            bigquery_dtype = "FLOAT64"
        elif postgres_dtype == 'date':
            bigquery_dtype = "DATE"
        elif postgres_dtype.startswith('timestamp'):
            bigquery_dtype = "TIMESTAMP"
        else:
            bigquery_dtype = "STRING"
        output_cols.append({ "name": column_name.lower(), "type": bigquery_dtype, "mode": bigquery_mode })
    logging.info(output_cols)
    return output_cols
def get_bq_high_watermark(gcp_project, source_table, merge_schema, source_filter_column):
    """
    Get the columns with data types for the merge table
    """
    client = bigquery.Client(project=gcp_project)
    max_date_sql = f"""select CAST(max({source_filter_column}) as TIMESTAMP) as max_date from `{gcp_project}.{merge_schema}.{source_table}`"""
    logging.info("high watermark sql = %s", max_date_sql)
    bq_job = client.query(max_date_sql)
    max_date_rows = bq_job.result()
    max_date = None
    if max_date_rows.total_rows > 0:
        for result_row in max_date_rows:
            max_date = result_row.max_date
        logging.info("high watermark = %s", max_date.strftime('%Y-%m-%d %H:%M:%S.%f'))
        return max_date.strftime('%Y-%m-%d %H:%M:%S.%f')
    else:
        logging.info("No max date found")
        return None

def set_high_watermark(gcp_project, metadata_collection, source_connection, source_schema, source_table, merge_schema, source_filter_column, **kwargs):
    """
    Set the high_watermark metadata element from the target database table
    """
    #pylint: disable=W0613
    fs_db = firestore.Client()
    doc_ref = fs_db.collection(metadata_collection).document(f'{source_connection}.{source_schema}.{source_table}')
    high_watermark = get_bq_high_watermark(gcp_project=gcp_project,source_table=source_table, merge_schema=merge_schema, source_filter_column=source_filter_column)
    data = {
        u'high_watermark': f"{high_watermark}",
        u'target_schema_table': f"{merge_schema}.{source_table}",
        u'id': f"{source_connection}.{source_schema}.{source_table}"
    }
    doc_ref.set(data, merge=True)
    logging.info("Set high_watermark='%s' from %s in %s.%s", high_watermark, source_filter_column, merge_schema, source_table)
    return high_watermark

def get_high_watermark(gcp_project, metadata_collection, source_connection, source_schema, source_table, merge_schema, source_filter_column):
    """
    Retrieve the high_watermark from Firestore or database if it doesn't exist
    """
    fs_db = firestore.Client()
    doc_ref = fs_db.collection(metadata_collection).document(f'{source_connection}.{source_schema}.{source_table}')
    high_wm_doc = doc_ref.get()
    found = False
    high_watermark = ""
    if high_wm_doc.exists:
        metadata = high_wm_doc.to_dict()
        if 'high_watermark' in metadata:
            high_watermark = metadata['high_watermark']
            found = True
            logging.info("Retrieved high_watermark %s from metadata", high_watermark)
    if not found:
        high_watermark = set_high_watermark(gcp_project, metadata_collection, source_connection, source_schema, source_table, merge_schema, source_filter_column)
    return high_watermark

def get_incoming_table_schema(gcp_project, source_table, incoming_bq_schema):
    """
    Get the columns with data types for the incoming table
    """
    logging.info("Project=%s", gcp_project)
    client = bigquery.Client(project=gcp_project)
    incoming_bq_schema_sql = f"""
    select column_name, data_type
    from `{gcp_project}.{incoming_bq_schema}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{source_table}'
    order by ordinal_position
    """
    logging.info(incoming_bq_schema_sql)
    incoming_job = client.query(incoming_bq_schema_sql)
    incoming_results = incoming_job.to_dataframe()
    return incoming_results

def get_merge_table_schema(gcp_project, source_table, merge_bq_schema):
    """
    Get the columns with data types for the merge table
    """
    client = bigquery.Client(project=gcp_project)
    merge_bq_schema_sql = f"""
    select column_name, data_type
    from `{gcp_project}.{merge_bq_schema}.INFORMATION_SCHEMA.COLUMNS`
    where table_name = '{source_table}'
    order by ordinal_position
    """
    merge_job = client.query(merge_bq_schema_sql)
    merge_results = merge_job.to_dataframe()
    return merge_results

def get_insert_sql_for_append(gcp_project, source_table, incoming_bq_schema, merge_bq_schema, target_project=None):
    """
    Return SQL for just appending to a target table
    """
    logging.info("Before target_project=%s", target_project)
    if target_project is None:
        target_project = gcp_project
    logging.info("After target_project=%s", target_project)
    incoming_results = get_incoming_table_schema(gcp_project, source_table, incoming_bq_schema)
    merge_results = get_merge_table_schema(target_project, source_table, merge_bq_schema)
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    insert_sql = f"INSERT INTO `{target_project}.{merge_bq_schema}.{source_table}` ("
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        insert_sql = insert_sql + f"`{col}`"
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + ")\n"
    i = 0
    insert_sql = insert_sql + "SELECT "
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        if incoming_cols['data_type'][idx] != merge_cols['data_type'][idx]:
            if incoming_cols['data_type'][idx] == 'INT64' and (merge_cols['data_type'][idx] == "DATE" or
                merge_cols['data_type'][idx] == "DATETIME" or
                merge_cols['data_type'][idx] == "TIMESTAMP"):
                col_str = f"""CAST(TIMESTAMP_MILLIS(CASE WHEN `{col}` < UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") THEN UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") ELSE `{col}` END ) as {merge_cols['data_type'][idx]}) as `{col}`"""
            else:
                col_str = f"SAFE_CAST(`{col}` as {merge_cols['data_type'][idx]}) as `{col}`"
        else:
            col_str = f"`{col}`"
        insert_sql = insert_sql + col_str
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + "\n"
    insert_sql = insert_sql + f"FROM `{gcp_project}.{incoming_bq_schema}.{source_table}`;"
    logging.info(insert_sql)
    return insert_sql

def get_insert_sql_for_multipart_append(gcp_project, source_table, staging_table, incoming_bq_schema, merge_bq_schema):
    """
    Return the not matched insert SQL block
    """
    incoming_results = get_incoming_table_schema(gcp_project, staging_table, incoming_bq_schema)
    merge_results = get_merge_table_schema(gcp_project, source_table, merge_bq_schema)
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    insert_sql = f"INSERT INTO `{gcp_project}.{merge_bq_schema}.{source_table}` ("
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        insert_sql = insert_sql + f"`{col}`"
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + ")\n"
    i = 0
    insert_sql = insert_sql + "SELECT "
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        if incoming_cols['data_type'][idx] != merge_cols['data_type'][idx]:
            if incoming_cols['data_type'][idx] == 'INT64' and (merge_cols['data_type'][idx] == "DATE" or
                merge_cols['data_type'][idx] == "DATETIME" or
                merge_cols['data_type'][idx] == "TIMESTAMP"):
                col_str = f"""CAST(TIMESTAMP_MILLIS(CASE WHEN `{col}` < UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") THEN UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") ELSE `{col}` END ) AS {merge_cols['data_type'][idx]}) AS `{col}`"""
            else:
                col_str = f"SAFE_CAST(`{col}` AS {merge_cols['data_type'][idx]}) AS `{col}`"
        else:
            col_str = f"`{col}`"
        insert_sql = insert_sql + col_str
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + "\n"
    insert_sql = insert_sql + f"FROM `{gcp_project}.{incoming_bq_schema}.{staging_table}`;"
    logging.info(insert_sql)
    return insert_sql

def get_insert_sql_for_full_refresh(gcp_project, source_table, incoming_bq_schema, merge_bq_schema):
    """
    Return the not matched insert SQL block
    """
    incoming_results = get_incoming_table_schema(gcp_project, source_table, incoming_bq_schema)
    merge_results = get_merge_table_schema(gcp_project, source_table, merge_bq_schema)
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    insert_sql = f"TRUNCATE TABLE `{gcp_project}.{merge_bq_schema}.{source_table}`;\n"
    insert_sql = insert_sql + f"INSERT INTO `{gcp_project}.{merge_bq_schema}.{source_table}` ("
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        insert_sql = insert_sql + f"`{col}`"
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + ")\n"
    i = 0
    insert_sql = insert_sql + "SELECT "
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        if incoming_cols['data_type'][idx] != merge_cols['data_type'][idx]:
            if incoming_cols['data_type'][idx] == 'INT64' and (merge_cols['data_type'][idx] == "DATE" or
                merge_cols['data_type'][idx] == "DATETIME" or
                merge_cols['data_type'][idx] == "TIMESTAMP"):
                col_str = f"""CAST(TIMESTAMP_MILLIS(CASE WHEN `{col}` < UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") THEN UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") ELSE `{col}` END ) AS {merge_cols['data_type'][idx]}) AS `{col}`"""
            else:
                col_str = f"SAFE_CAST(`{col}` as {merge_cols['data_type'][idx]}) as `{col}`"
        else:
            col_str = f"`{col}`"
        insert_sql = insert_sql + col_str
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + "\n"
    insert_sql = insert_sql + f"FROM `{gcp_project}.{incoming_bq_schema}.{source_table}`;"
    logging.info(insert_sql)
    return insert_sql

def get_join_sql(metadata_collection, src_connection, src_schema, src_table, incoming_results, merge_results, merge_schema):
    """
    Return the merge join SQL
    """
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    on_sql = ""
    merge_keys = get_merge_keys(metadata_collection, src_connection, src_schema, src_table, merge_schema)
    for key in merge_keys:
        if i > 0:
            on_sql = on_sql + "AND "
        if incoming_cols['data_type'][key] != merge_cols['data_type'][key]:
            on_sql = on_sql + f"""CAST(incoming_table.`{key}` as {merge_cols['data_type'][key]}) = merge_table.`{key}`\n"""
        else:
            on_sql = on_sql + f"""incoming_table.`{key}` = merge_table.`{key}`\n"""
        i = i + 1
    return on_sql

def get_join_sql_for_oracle_sqoop(metadata_collection, src_connection, src_schema, src_table, incoming_results, merge_results, merge_schema):
    """
    Return the merge join SQL
    """
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    on_sql = ""
    merge_keys = get_merge_keys(metadata_collection, src_connection, src_schema, src_table, merge_schema)
    for key in merge_keys:
        if i > 0:
            on_sql = on_sql + "AND "
        if incoming_cols['data_type'][key.upper()] != merge_cols['data_type'][key]:
            if incoming_cols['data_type'][key.upper()] == 'INT64' and (merge_cols['data_type'][key] == "DATE" or
                merge_cols['data_type'][key] == "DATETIME" or
                merge_cols['data_type'][key] == "TIMESTAMP"):
                on_sql = on_sql + f""" CAST(TIMESTAMP_MILLIS(CASE WHEN incoming_table.`{key}` < UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") THEN UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") ELSE incoming_table.`{key}` END) AS {merge_cols['data_type'][key]}) = merge_table.`{key}` \n"""
            else:
                on_sql = on_sql + f"""CAST(incoming_table.`{key}` as {merge_cols['data_type'][key]}) = merge_table.`{key}`\n"""
        else:
            on_sql = on_sql + f"""incoming_table.`{key}` = merge_table.`{key}`\n"""
        i = i + 1
    return on_sql

def get_join_sql_for_as400_sqoop(metadata_collection, src_connection, src_schema, src_table, incoming_results, merge_results, merge_schema):
    """
    Return the merge join SQL
    """
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    on_sql = ""
    merge_keys = get_merge_keys(metadata_collection, src_connection, src_schema, src_table, merge_schema)
    for key in merge_keys:
        if i > 0:
            on_sql = on_sql + "AND "
        if incoming_cols['data_type'][key.upper()] != merge_cols['data_type'][key]:
            if incoming_cols['data_type'][key.upper()] == 'INT64' and (merge_cols['data_type'][key] == "DATE" or
                merge_cols['data_type'][key] == "DATETIME" or
                merge_cols['data_type'][key] == "TIMESTAMP"):
                on_sql = on_sql + f""" CAST(TIMESTAMP_MILLIS(CASE WHEN incoming_table.`{key}` < UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") THEN UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") ELSE incoming_table.`{key}` END) AS {merge_cols['data_type'][key]}) = merge_table.`{key}` \n"""
            else:
                on_sql = on_sql + f"""CAST(incoming_table.`{key}` as {merge_cols['data_type'][key]}) = merge_table.`{key}`\n"""
        else:
            on_sql = on_sql + f"""incoming_table.`{key}` = merge_table.`{key}`\n"""
        i = i + 1
    return on_sql

def get_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_results, merge_results, merge_schema):
    """
    Return the WHEN MATCHED SQL block
    """
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    set_sql = ""
    merge_keys = get_merge_keys(metadata_collection, src_connection, src_schema, src_table, merge_schema)
    for idx in merge_results.index:
        if merge_results['column_name'][idx] not in merge_keys:
            col = merge_results['column_name'][idx]
            if i > 0:
                set_sql = set_sql + ", "
            if incoming_cols['data_type'][idx] != merge_cols['data_type'][idx]:
                if incoming_cols['data_type'][idx] == 'INT64' and (merge_cols['data_type'][idx] == "DATE" or
                    merge_cols['data_type'][idx] == "DATETIME" or
                    merge_cols['data_type'][idx] == "TIMESTAMP"):
                    set_sql = set_sql + f""" merge_table.`{col}` = CAST(TIMESTAMP_MILLIS(CASE WHEN incoming_table.`{col}` < UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") THEN UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") ELSE incoming_table.`{col}` END) AS {merge_cols['data_type'][idx]})\n"""
                else:
                    set_sql = set_sql + f""" merge_table.`{col}` = SAFE_CAST(incoming_table.`{col}` as {merge_cols['data_type'][idx]})\n"""
            else:
                set_sql = set_sql + f""" merge_table.`{col}` = incoming_table.`{col}`\n"""
            i = i + 1
    return set_sql

def get_not_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_results, merge_results, merge_schema):
    """
    Return the not matched insert SQL block
    """
    incoming_cols = incoming_results.set_index('column_name')
    merge_cols = merge_results.set_index('column_name')
    i = 0
    insert_sql = "INSERT ("
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        insert_sql = insert_sql + f"`{col}`"
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + ")\n"
    i = 0
    insert_sql = insert_sql + "VALUES ("
    for idx in merge_results.index:
        i = i + 1
        col = merge_results['column_name'][idx]
        if incoming_cols['data_type'][idx] != merge_cols['data_type'][idx]:
            if incoming_cols['data_type'][idx] == 'INT64' and (merge_cols['data_type'][idx] == "DATE" or
                merge_cols['data_type'][idx] == "DATETIME" or
                merge_cols['data_type'][idx] == "TIMESTAMP"):
                col_str = f"""CAST(TIMESTAMP_MILLIS(CASE WHEN incoming_table.`{col}` < UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") THEN UNIX_MILLIS(TIMESTAMP "1900-01-01 00:00:00+00") ELSE incoming_table.`{col}` END ) AS {merge_cols['data_type'][idx]})"""
            else:
                col_str = f"SAFE_CAST(incoming_table.`{col}` as {merge_cols['data_type'][idx]})"
        else:
            col_str = f"incoming_table.`{col}`"
        insert_sql = insert_sql + col_str
        if i < len(merge_results):
            insert_sql = insert_sql + ", \n"
    insert_sql = insert_sql + ")\n"
    return insert_sql

def get_merge_sql(gcp_project, metadata_collection, src_connection, src_schema, src_table, incoming_schema, merge_schema, src_filter_column):
    """
    Return the merge statement SQL
    """
    incoming_metadata = get_incoming_table_schema(gcp_project, src_table, incoming_schema)
    merge_metadata = get_merge_table_schema(gcp_project, src_table, merge_schema)

    merge_sql = f"""
    MERGE `{gcp_project}.{merge_schema}.{src_table}` merge_table
    USING `{gcp_project}.{incoming_schema}.{src_table}` incoming_table
    ON
    {get_join_sql(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}WHEN MATCHED AND CAST(incoming_table.{src_filter_column} AS TIMESTAMP) > CAST(merge_table.{src_filter_column} AS TIMESTAMP) THEN
    UPDATE SET
    {get_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}WHEN NOT MATCHED THEN
    {get_not_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}
    """
    logging.info(merge_sql)
    return merge_sql

def get_merge_sql_for_oracle_sqoop(gcp_project, metadata_collection, src_connection, src_schema, src_table, incoming_schema, stg_table, merge_schema, src_filter_column):
    """
    Return the merge statement SQL
    """
    incoming_metadata = get_incoming_table_schema(gcp_project, stg_table, incoming_schema)
    merge_metadata = get_merge_table_schema(gcp_project, src_table, merge_schema)

    merge_sql = f"""
    MERGE `{gcp_project}.{merge_schema}.{src_table}` merge_table
    USING `{gcp_project}.{incoming_schema}.{stg_table}` incoming_table
    ON
    {get_join_sql_for_oracle_sqoop(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}WHEN MATCHED AND TIMESTAMP_MILLIS(incoming_table.{src_filter_column}) > CAST(merge_table.{src_filter_column} AS TIMESTAMP) THEN
    UPDATE SET
    {get_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}WHEN NOT MATCHED THEN
    {get_not_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}
    """
    logging.info(merge_sql)
    return merge_sql

def get_merge_sql_for_as400_sqoop(gcp_project, metadata_collection, src_connection, src_schema, src_table, incoming_schema, stg_table, merge_schema, tgt_table, src_filter_column):
    """
    Return the merge statement SQL
    """
    incoming_metadata = get_incoming_table_schema(gcp_project, stg_table, incoming_schema)
    merge_metadata = get_merge_table_schema(gcp_project, tgt_table, merge_schema)

    merge_sql = f"""
    MERGE `{gcp_project}.{merge_schema}.{tgt_table}` merge_table
    USING `{gcp_project}.{incoming_schema}.{stg_table}` incoming_table
    ON
    {get_join_sql_for_as400_sqoop(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}WHEN MATCHED THEN
    UPDATE SET
    {get_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}WHEN NOT MATCHED THEN
    {get_not_matched_sql(metadata_collection, src_connection, src_schema, src_table, incoming_metadata, merge_metadata, merge_schema)}
    """
    logging.info(merge_sql)
    return merge_sql

def gcs_data_exists(final_bucket, gcs_path, exists_task_id, not_exists_task_id):
    """Branch op check for data"""
    gcs_hook = GCSHook()
    search_dir, search_file = ntpath.split(gcs_path)
    if search_file.find(".") > 0:
        obj_list = gcs_hook.list(final_bucket, prefix=search_dir)
        obj_list = fnmatch.filter(obj_list, search_file)
    else:
        search_dir = search_dir + "/" + search_file
        obj_list = gcs_hook.list(final_bucket, prefix=search_dir)
    if len(obj_list) > 0:
        return exists_task_id
    else:
        return not_exists_task_id

def bq_table_exists(table_id, exists_task_id, not_exists_task_id):
    """
    Check if target BQ table exists
    """
    client = bigquery.Client()

    try:
        client.get_table(table_id)  # Make an API request.
        return exists_task_id
    except google.api_core.exceptions.NotFound:
        return not_exists_task_id

def get_pf_start_date(years_ago, dt):
    """Get the start date of the specified performance year"""
    delta_period = dt - relativedelta.relativedelta(years=years_ago)
    if delta_period.month >= 9:
        start_date = datetime(delta_period.year, 9, 1)
    else:
        start_date = datetime((delta_period.year -1), 9, 1)
    return start_date

def get_pf_start_yearmonth(years_ago, dt):
    """Get the start yearmonth of the specified performance year"""
    delta_period = dt - relativedelta.relativedelta(years=years_ago)
    if delta_period.month >= 9:
        start_date = datetime(delta_period.year, 9, 1)
    else:
        start_date = datetime((delta_period.year -1), 9, 1)
    return start_date.strftime("%Y%m")

def get_months_ago_yearmonth(months_ago, dt):
    """Return the yearmonth from months_ago, releative to dt"""
    delta_period = dt - relativedelta.relativedelta(months=months_ago)
    return delta_period.strftime("%Y%m")
