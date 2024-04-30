"""
Copy incoming extract to
"""
import gcsfs
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class GCSCopyIncomingToFinalOperator(BaseOperator):
    """
    Get specified table and persist to parquet in the scratch landing zone bucket

    :param gcp_project: GCP project id to use.
    :type gcp_project: str
    :param source_connection: Source connection ID.
    :type source_connection: str
    :param source_schema: Oracle source schema.
    :type source_schema: str
    :param source_table: Oracle source table.
    :type source_table: str
    :param partition: partition for the GCS prefix.
    :type partition: str
    :param sync_method: Batch synchronization method.
    :type sync_method: str
    :param incoming_bucket: Staging GCS bucket.
    :type incoming_bucket: str
    :param final_bucket: Destination GCS bucket.
    :type final_bucket: str
    :param gcp_token: gcsfs auth token.
    :type gcp_token: str
    """

    template_fields = ['partition']

    @apply_defaults
    def __init__(self,
                 *args,
                 gcp_project=None,
                 source_connection=None,
                 source_schema=None,
                 source_table=None,
                 partition=None,
                 sync_method='CUSTOM',
                 incoming_bucket=None,
                 final_bucket=None,
                 gcp_token='cloud',
                 **kwargs
                ):
        super().__init__(*args, **kwargs)
        self.gcp_project = gcp_project
        self.source_connection = source_connection
        self.source_schema = source_schema
        self.source_table = source_table
        self.partition = partition
        self.sync_method = sync_method
        self.incoming_bucket = incoming_bucket
        self.final_bucket = final_bucket
        self.gcp_token = gcp_token

    def execute(self, context):
        self.log.info("gcp_project=%s", self.gcp_project)
        self.log.info("source_connection=%s", self.source_connection)
        self.log.info("source_schema=%s", self.source_schema)
        self.log.info("source_table=%s", self.source_table)
        self.log.info("partition=%s", self.partition)
        self.log.info("sync_method=%s", self.sync_method)
        self.log.info("incoming_bucket=%s", self.incoming_bucket)
        self.log.info("final_bucket=%s", self.final_bucket)
        self.log.info("gcp_token=%s", self.gcp_token)
        if self.source_schema is None:
            prefix = f"gs://{self.final_bucket}/{self.source_connection}/{self.sync_method}/{self.source_table}/{self.partition}"
        else:
            prefix = f"gs://{self.final_bucket}/{self.source_connection}/{self.sync_method}/{self.source_schema}/{self.source_table}/{self.partition}"
        self.log.info("Clearing %s", prefix)
        gcs_fs = gcsfs.GCSFileSystem(token=self.gcp_token)
        if gcs_fs.exists(f'{prefix}'):
            gcs_fs.rm(f'{prefix}', True)
        if self.source_schema is None:
            prefix = f"{self.source_connection}/{self.sync_method}/{self.source_table}/{self.partition}"
        else:
            prefix = f"{self.source_connection}/{self.sync_method}/{self.source_schema}/{self.source_table}/{self.partition}"
        self.log.info("Looking for files in %s", prefix)
        gcs_hook = GCSHook()
        obj_list = gcs_hook.list(self.incoming_bucket, prefix=prefix)
        self.log.info("obj_list=%s", obj_list)
        for obj in obj_list:
            if obj.endswith(".avro") or obj.endswith(".parquet") or obj.endswith(".csv"):
                gcs_hook.rewrite(self.incoming_bucket, obj, self.final_bucket, obj)
                self.log.info("Copied gs://%s", self.incoming_bucket + "/" + obj)
                self.log.info("to gs://%s", self.final_bucket + "/" + obj)
