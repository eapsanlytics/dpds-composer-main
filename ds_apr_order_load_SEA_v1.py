"""
DS APR:  Load order data into GCS - SEA (SG, MY, BN, TW, KR)
consolidated
"""
from datetime import datetime, timedelta, date
from io import StringIO
import time

import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from ingestion.alerts.custom_ms_teams_webhook_operator import CustomMSTeamsWebhookOperator
from ingestion import bq_utility

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"
DAG_CONFIG = Variable.get('DS_APR_ORDER_DAG_CONFIG', deserialize_json=True)

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

ORDERS_BY_COUNTRY_BASH_TEMPLATE = """bq query --nouse_legacy_sql --project_id={{params.project_id}} "EXPORT DATA OPTIONS(
uri='gs://{{params.gcs_bucket}}/{{params.folder}}/orders/orders_*.csv',
format='CSV',
overwrite=true,
header=true,
field_delimiter=';') AS
SELECT
ord_dt_key_no,
ord_id,
global_account_id,
country_name,
demand_ord_account_id AS ord_account_id,
ord_bus_natr_cd,
ship_bus_natr_cd,
ord_item_cd,
ord_item_base_cd,
ord_qty,
catalog_ln_lc_net,
adj_ln_lc_net,
catalog_ln_usd_net,
prm_ln_usd_net,
cpn_ln_usd_net,
adj_ln_usd_net,
account_type_ord
FROM
{{params.dataset_table}}
WHERE
country_name = '{{params.filter}}' " """

ORDER_DELETE_TEMPLATE = """gsutil -m rm -r gs://{{params.gcs_bucket}}/{{params.folder}}/orders/"""

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

with DAG('ds_apr_order_load_SEA_v1',
         description='Workflow to load order data into gcs for APR',
         schedule_interval="13 8 * * 1",
         #schedule_interval="@once",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         #start_date=days_ago(1),
         start_date=datetime(2022,3,1),
         catchup=False) as dag:

    START = DummyOperator(task_id="start")
    END = DummyOperator(task_id="end")
    COUNTRY_LIST = pd.read_csv(StringIO(Variable.get('DS_APR_HEAP_SEA')), header='infer')

    START

    for index, row in COUNTRY_LIST.iterrows():
        gcs_delete_order_data = BashOperator(
            task_id=f"gcs_delete_{row['country']}_view_data",
            bash_command=ORDER_DELETE_TEMPLATE,
            params={
                "country":row["country"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket']
            }
        )
        gcs_load_order_data = BashOperator(
            task_id=f"gcs_create_{row['country']}_order_data",
            bash_command=ORDERS_BY_COUNTRY_BASH_TEMPLATE,
            params={
                "project_id":DAG_CONFIG['PROJECT_ID'],
                "country":row["country"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "dataset_table":DAG_CONFIG['DATASET_TABLE'],
                "filter":row['filter']
            }
        )
        START >> gcs_delete_order_data >> gcs_load_order_data >> END
