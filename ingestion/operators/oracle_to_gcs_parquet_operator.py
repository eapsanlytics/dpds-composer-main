"""
Get specified table and persist to parquet in the scratch landing zone bucket
"""
from datetime import timedelta

import cx_Oracle
import gcsfs
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ingestion import metadata

class OracleToGCSParquetOperator(BaseOperator):
    """
    Get specified table and persist to parquet in the scratch landing zone bucket

    :param gcp_project: GCP project id to use.
    :type gcp_project: str
    :param metadata_collection: Firestore collection id.
    :type metadata_collection: str
    :param source_connection: Oracle connection ID.
    :type source_connection: str
    :param source_schema: Oracle source schema.
    :type source_schema: str
    :param source_table: Oracle source table.
    :type source_table: str
    :param source_filter_column: Oracle filter column.
    :type source_filter_column: str
    :param merge_schema: BigQuery merge dataset name.
    :type merge_schema: str
    :param sync_method: Batch synchronization method.
    :type sync_method: str
    :param chunk_size: Numbers of rows to chunk out at a time.
    :type chunk_size: int
    :param staging_bucket: GCS bucket to staging the parquet files in.
    :type staging_bucket: str
    :param gcp_token: gcsfs auth token.
    :type gcp_token: str
    """
    #pylint: disable=too-many-locals
    @apply_defaults
    def __init__(self,
                 gcp_project,
                 metadata_collection,
                 source_connection,
                 source_schema,
                 source_table,
                 source_filter_column,
                 merge_schema,
                 sync_method,
                 chunk_size,
                 staging_bucket,
                 gcp_token,
                 *args,
                 **kwargs):
        super(OracleToGCSParquetOperator, self).__init__(*args, **kwargs)
        self.gcp_project = gcp_project
        self.metadata_collection = metadata_collection
        self.source_connection = source_connection
        self.source_schema = source_schema
        self.source_table = source_table
        self.source_filter_column = source_filter_column
        self.merge_schema = merge_schema
        self.sync_method = sync_method
        self.chunk_size = chunk_size
        self.staging_bucket = staging_bucket
        self.gcp_token = gcp_token

    def execute(self, context):
        #pylint: disable=too-many-locals
        connection = BaseHook.get_connection(self.source_connection)
        oracle_conn = cx_Oracle.connect(connection.login, connection.password, connection.host, encoding="UTF-8")
        ds_nodash = context['execution_date'].strftime('%Y%m%d')
        next_day = (context['execution_date'] + timedelta(days=1)).strftime('%Y%m%d')
        chunk_size = self.chunk_size
        reference_dtypes = metadata.get_dtypes(self.metadata_collection, self.source_connection, self.source_schema, self.source_table, self.merge_schema)
        sql = ""
        #pylint: disable=line-too-long
        if self.sync_method == "APPEND":
            high_watermark = metadata.get_high_watermark(
                gcp_project=self.gcp_project,
                metadata_collection=self.metadata_collection,
                source_connection=self.source_connection,
                source_schema=self.source_schema,
                source_table=self.source_table,
                merge_schema=self.merge_schema,
                source_filter_column=self.source_filter_column
            )
            sql = f"""select * from {self.source_schema.upper()}.{self.source_table.upper()} where {self.source_filter_column} > TO_TIMESTAMP('{high_watermark}','yyyy-mm-dd hh24:mi:ss.ff6')"""
        elif self.sync_method == "FULL":
            sql = f'''select * from {self.source_schema.upper()}.{self.source_table.upper()}'''
        elif self.sync_method == "INCREMENTAL":
            sql = f'''select * from {self.source_schema.upper()}.{self.source_table.upper()} where {self.source_filter_column} >= TO_DATE('{ds_nodash}', 'YYYYMMDD') and {self.source_filter_column} < TO_DATE('{next_day}', 'YYYYMMDD')'''
        partition = context['execution_date'].strftime('year=%Y/month=%m/day=%d')
        prefix = f"gs://{self.staging_bucket}/{self.source_connection}/{self.sync_method}/{self.source_schema}/{self.source_table}/{partition}"
        self.log.info(sql)
        self.log.info(prefix)
        gcs_fs = gcsfs.GCSFileSystem(token=self.gcp_token)
        if gcs_fs.exists(f'{prefix}'):
            gcs_fs.rm(f'{prefix}', True)
        rows = 0
        for chunk in pd.read_sql_query(sql, oracle_conn, chunksize=chunk_size):
            chunk.columns = chunk.columns.str.lower()
            self.log.info('Chunk=%s', chunk.dtypes.apply(lambda x: x.name).to_dict())
            chunk = chunk.astype(reference_dtypes)
            parquet_schema = pa.Schema.from_pandas(df=chunk)
            for col in reference_dtypes:
                idx = parquet_schema.get_field_index(col)
                if reference_dtypes[col] == "object":
                    chunk[col] = chunk[col].apply(lambda v: str(v) if not pd.isnull(v) else None)
                    parquet_schema = parquet_schema.set(idx,pa.field(col, pa.string()))
            table = pa.Table.from_pandas(pd.DataFrame(chunk), schema=parquet_schema)
            pq.write_to_dataset(table, root_path=prefix, filesystem=gcs_fs)
            rows = rows + len(chunk)
            self.log.info('Wrote %s rows', rows)
