"""
Keep GCS buckets syncronized
"""

import os
from datetime import datetime,timedelta
from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSSynchronizeBucketsOperator
#from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
#from airflow.utils.dates import days_ago

from ingestion.alerts.custom_ms_teams_webhook_operator import \
    CustomMSTeamsWebhookOperator

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

BUCKET_SRC = os.environ.get("GCP_GCS_BUCKET_SRC", "ds_migration_prd")
BUCKET_DST = os.environ.get("GCP_GCS_BUCKET_DST", "migration_prd_runs")

MACRO_BUCKET_SRC = os.environ.get("GCP_GCS_BUCKET_SRC", "macro_fcst_prd")
MACRO_BUCKET_DST = os.environ.get("GCP_GCS_BUCKET_DST", "macro_prod_runs")

def msteams_task_fail_alert(context):
    """
    Send a task failure alert to Microsoft Teams
    """
    failed_alert = CustomMSTeamsWebhookOperator(
        task_id='task_failure',
        http_conn_id=MSTEAMS_ALERT_CONN_ID
    )
    return failed_alert.execute(context=context)

def msteams_task_retry_alert(context):
    """
    Send a task retry alert to Microsoft Teams
    """
    retry_alert = CustomMSTeamsWebhookOperator(
        task_id='task_retry',
        http_conn_id=MSTEAMS_ALERT_CONN_ID
    )
    return retry_alert.execute(context=context)

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['michael.janzen@amway.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'weight_rule': 'upstream',
    'on_retry_callback': msteams_task_retry_alert,
    'on_failure_callback': msteams_task_fail_alert
}

with models.DAG(
    "gcs_to_gcs_bucket_sync",
    description='sync buckets in gcs',
    schedule_interval="0 6 6,12 * *",
    default_args=DEFAULT_ARGS,
    concurrency=12,
    max_active_runs=1,
    start_date=datetime(2021,8,1),
    catchup=False
) as dag:

    sync_migration_bucket = GCSSynchronizeBucketsOperator(
        task_id="sync_migration_bucket",
        source_bucket=BUCKET_SRC,
        destination_bucket=BUCKET_DST
    )

    sync_macro_bucket = GCSSynchronizeBucketsOperator(
        task_id="sync_macro_bucket",
        source_bucket=MACRO_BUCKET_SRC,
        destination_bucket=MACRO_BUCKET_DST
    )
    #pylint: disable=pointless-statement
    sync_migration_bucket >> sync_macro_bucket
