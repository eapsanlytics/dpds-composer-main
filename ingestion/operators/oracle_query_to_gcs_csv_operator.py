"""
Get specified table and persist to parquet in the scratch landing zone bucket
"""
from io import StringIO

import csv
import gcsfs
from airflow.hooks.oracle_hook import OracleHook
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults

class OracleQueryToGCSCSVOperator(BaseOperator):
    """
    Run the specified query and persist to CSV in the scratch landing zone bucket

    :param gcp_project: GCP project id to use.
    :type gcp_project: str
    :param staging_bucket: GCS bucket to stage the csv files in.
    :type staging_bucket: str
    :param source_connection: Oracle connection ID.
    :type source_connection: str
    :param source_table: Oracle source table.
    :type source_table: str
    :param sync_method: Batch synchronization method.
    :type sync_method: str
    :param gcp_token: gcsfs auth token.
    :type gcp_token: str
    :param separator: csv separator.
    :type separator: str
    :param index: include pandas index
    :type index: boolean
    :param header: output csv header
    :type header: boolean
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)

    #pylint: disable=too-many-locals
    @apply_defaults
    def __init__(self,
                 gcp_project,
                 staging_bucket,
                 source_connection,
                 source_table,
                 sync_method,
                 sql,
                 gcp_token,
                 *args,
                 separator=",",
                 index=False,
                 header=True,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.gcp_project = gcp_project
        self.source_connection = source_connection
        self.source_table = source_table
        self.sync_method = sync_method
        self.staging_bucket = staging_bucket
        self.sql = sql
        self.gcp_token = gcp_token
        self.separator = separator
        self.index = index
        self.header = header

    def execute(self, context):
        partition = context['execution_date'].strftime('year=%Y/month=%m/day=%d')
        prefix = f"gs://{self.staging_bucket}/{self.source_connection}/{self.sync_method}/{self.source_table}/{partition}"
        self.log.info(prefix)
        gcs_fs = gcsfs.GCSFileSystem(token=self.gcp_token)
        if gcs_fs.exists(f'{prefix}'):
            gcs_fs.rm(f'{prefix}', True)
        oracle_hook = OracleHook(self.source_connection)
        df_results = oracle_hook.get_pandas_df(sql=self.sql)
        csv_buffer = StringIO()
        df_results.to_csv(csv_buffer, sep=self.separator, index=self.index, header=self.header, quoting=csv.QUOTE_ALL)
        gcs_hook = GCSHook()
        object_name = f"{self.source_connection}/{self.sync_method}/{self.source_table}/{partition}/{self.source_table}.csv"
        gcs_hook.upload(
            bucket_name=self.staging_bucket,
            object_name=object_name,
            data=csv_buffer.getvalue()
        )
