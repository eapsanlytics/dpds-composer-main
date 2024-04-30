"""BigQuery related subroutines"""

import hashlib
import logging
import re
import pytz
import uuid

from datetime import datetime, timedelta
from time import sleep
from google.cloud import bigquery

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from ingestion import metadata

def call_bq_job_insert_operator_for_append(task_id, gcp_project, source_table, incoming_bq_schema, merge_bq_schema, target_project=None, **kwargs):
    """
    Wrap BigQueryInsertJobOperator <--- NOPE
    2020-12-18 - Call the BigQueryHook.run_query function as the insert job wasn't actually inserting anything
    """
    logging.info("target_project=%s", target_project)
    sql = metadata.get_insert_sql_for_append(gcp_project, source_table, incoming_bq_schema, merge_bq_schema, target_project)
    client = bigquery.Client()
    job = client.query(sql)
    while job.running():
        logging.info("Waiting for query")
        sleep(1)
    logging.info("Job state=%s", job.state)
    logging.info("Inserted %s rows", job.num_dml_affected_rows)
    client.close()

def call_bq_job_insert_operator_for_multipart_append(gcp_project, source_table, staging_table, incoming_bq_schema, merge_bq_schema, **kwargs):
    """
    Run the append SQL for the multipart append to BQ
    """
    sql = metadata.get_insert_sql_for_multipart_append(gcp_project, source_table, staging_table, incoming_bq_schema, merge_bq_schema)
    client = bigquery.Client()
    job = client.query(sql)
    while job.running():
        logging.info("Waiting for query")
        sleep(1)
    logging.info("Job state=%s", job.state)
    logging.info("Inserted %s rows", job.num_dml_affected_rows)
    client.close()

def get_job_id(task_id, exec_date):
    """Generate a unique bigquery job ID"""
    hash_base = str(uuid.uuid4())
    uniqueness_suffix = hashlib.md5(hash_base.encode()).hexdigest()
    exec_date = exec_date.isoformat()
    job_id = f"airflow_{task_id}_{exec_date}_{uniqueness_suffix}"
    job_id = re.sub(r"[:\-+.]", "_", job_id)
    return job_id

def call_bq_job_insert_operator_for_full_refresh(task_id, gcp_project, source_table, incoming_bq_schema, merge_bq_schema, **kwargs):
    """
    Wrap BigQueryInsertJobOperator
    """
    task = BigQueryInsertJobOperator(
        task_id=task_id,
        job_id=get_job_id(task_id, kwargs['execution_date']),
        configuration={
            "query": {
                "query": metadata.get_insert_sql_for_full_refresh(gcp_project, source_table, incoming_bq_schema, merge_bq_schema),
                "useLegacySql": "False",
            }
        },
        location="US",
    )
    task.execute(context=kwargs)

#pylint: disable=too-many-arguments
def call_bq_job_insert_operator_for_merge(task_id, gcp_project, metadata_collection, source_connection, source_schema, source_table, incoming_bq_schema, merge_bq_schema, source_filter_column, **kwargs):
    """
    Wrap BigQueryInsertJobOperator
    """
    task = BigQueryInsertJobOperator(
        task_id=task_id,
        job_id=get_job_id(task_id, kwargs['execution_date']),
        configuration={
            "query": {
                "query": metadata.get_merge_sql(gcp_project, metadata_collection, source_connection, source_schema, source_table, incoming_bq_schema, merge_bq_schema, source_filter_column),
                "useLegacySql": "False",
            }
        },
        location="US",
    )
    task.execute(context=kwargs)

def call_bq_job_insert_operator_for_merge_oracle_sqoop(task_id, gcp_project, metadata_collection, source_connection, source_schema, source_table, incoming_bq_schema, staging_table, merge_bq_schema, source_filter_column, **kwargs):
    """
    Wrap BigQueryInsertJobOperator
    """
    task = BigQueryInsertJobOperator(
        task_id=task_id,
        job_id=get_job_id(task_id, kwargs['execution_date']),
        configuration={
            "query": {
                "query": metadata.get_merge_sql_for_oracle_sqoop(gcp_project, metadata_collection, source_connection, source_schema, source_table, incoming_bq_schema, staging_table, merge_bq_schema, source_filter_column),
                "useLegacySql": "False",
            }
        },
        location="US",
    )
    task.execute(context=kwargs)

def call_bq_job_insert_operator_for_merge_as400_sqoop(task_id, gcp_project, metadata_collection, source_connection, source_schema, source_table, incoming_bq_schema, staging_table, merge_bq_schema, target_table, source_filter_column, **kwargs):
    """
    Wrap BigQueryInsertJobOperator
    """
    task = BigQueryInsertJobOperator(
        task_id=task_id,
        job_id=get_job_id(task_id, kwargs['execution_date']),
        configuration={
            "query": {
                "query": metadata.get_merge_sql_for_as400_sqoop(gcp_project, metadata_collection, source_connection, source_schema, source_table, incoming_bq_schema, staging_table, merge_bq_schema, target_table, source_filter_column),
                "useLegacySql": "False",
            }
        },
        location="US",
    )
    task.execute(context=kwargs)

def remove_rows_for_partial_refresh(gcp_project, source_table, merge_bq_schema, refresh_col, refresh_months, **kwargs):
    """
    Remove data from the target table prior to partial refresh
    """
    start_period = metadata.get_months_ago_yearmonth(refresh_months, kwargs['execution_date'])
    sql = f"DELETE FROM `{gcp_project}.{merge_bq_schema}.{source_table}` WHERE {refresh_col.lower()} >= {start_period}"
    logging.info(sql)
    client = bigquery.Client()
    job = client.query(sql)
    while job.running():
        logging.info("Waiting for query")
        sleep(1)
    logging.info("Job state=%s", job.state)
    logging.info("Deleted %s rows", job.num_dml_affected_rows)
    client.close()

def remove_rows_for_1time_partial_refresh(gcp_project, source_table, merge_bq_schema, where_clause, **kwargs):
    """
    Remove data from the target table prior to partial refresh
    """
    sql = f"DELETE FROM `{gcp_project}.{merge_bq_schema}.{source_table}` WHERE {where_clause}"
    logging.info(sql)
    client = bigquery.Client()
    job = client.query(sql)
    while job.running():
        logging.info("Waiting for query")
        sleep(1)
    logging.info("Job state=%s", job.state)
    logging.info("Deleted %s rows", job.num_dml_affected_rows)
    client.close()

def set_table_expiration(gcp_project, dataset_id, table_name, expire_in_days):
    """Set the expiration time for a BigQuery table

    Args:
        gcp_project ([type]): [description]
        dataset_id ([type]): [description]
        table_name ([type]): [description]
        expire_in_days ([type]): [description]
    """
    client = BigQueryHook().get_client()
    dataset_ref = bigquery.DatasetReference(gcp_project, dataset_id)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)  # API request

    # set table to expire 5 days from now
    expiration = datetime.now(pytz.utc) + timedelta(days=expire_in_days)
    table.expires = expiration
    table = client.update_table(table, ["expires"])  # API request

def set_bigquery_batch_checkpoint(gcp_project, source_connection, source_schema, source_table, batch_timestamp, iteration, **kwargs):
    """
    Sets the last successful run timestamp for a particular batch table
    """

    BQ = bigquery.Client()
    
    if 'dev' in gcp_project:
        query = """
            UPDATE `amw-dna-ingestion-dev.batch_checkpointing.batch_checkpoint`
            SET last_successful_run = @batch_runtime
            WHERE source_system = @batch_source_connection and schema = @batch_schema and table = @batch_table;
        """
    elif 'prd' in gcp_project:
        query = """
            UPDATE `amw-dna-ingestion-prd.batch_checkpointing.batch_checkpoint`
            SET last_successful_run = @batch_runtime
            WHERE source_system = @batch_source_connection and schema = @batch_schema and table = @batch_table;
        """
    else:
        print("Unknown GCP Project...")

    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("batch_runtime", "TIMESTAMP", batch_timestamp),
                bigquery.ScalarQueryParameter("batch_source_connection", "STRING", source_connection),
                bigquery.ScalarQueryParameter("batch_schema", "STRING", source_schema),
                bigquery.ScalarQueryParameter("batch_table", "STRING", source_table),
            ]
        )
        query_job = BQ.query(query, job_config=job_config)  # API request
        print(query_job.result())  # Waits for query to finish
    except Exception as e:
        print('Exception')
        print(e)
        if iteration <= 6:
            sleep_duration = 2**iteration
            print('Waiting for ' + str(sleep_duration) + ' seconds before retrying')
            sleep(sleep_duration)
            iteration += 1
            set_bigquery_batch_checkpoint(gcp_project, source_connection, source_schema, source_table, batch_timestamp, iteration)
        else:
            print('Max retries exceeded, raising a failure')
            raise e

def get_bigquery_batch_checkpoint(gcp_project, source_connection, schema, table):
    """
    Retrieves the last successful run for a particular batch table
    """

    BQ = bigquery.Client()

    if 'dev' in gcp_project:
        query = """
            SELECT last_successful_run FROM `amw-dna-ingestion-dev.batch_checkpointing.batch_checkpoint`
            WHERE source_system = @batch_source_connection and schema = @batch_schema and table = @batch_table;
        """
    elif 'prd' in gcp_project:
        query = """
            SELECT last_successful_run FROM `amw-dna-ingestion-prd.batch_checkpointing.batch_checkpoint`
            WHERE source_system = @batch_source_connection and schema = @batch_schema and table = @batch_table;
        """
    else:
        print("Unknown GCP Project...")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("batch_source_connection", "STRING", source_connection),
            bigquery.ScalarQueryParameter("batch_schema", "STRING", schema),
            bigquery.ScalarQueryParameter("batch_table", "STRING", table),
        ]
    )
    query_job = BQ.query(query, job_config=job_config)  # API request
    results = query_job.result()  # Waits for query to finish
    for row in results:
        return row.last_successful_run