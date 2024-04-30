"""
DS APR:  Load order data into GCS - North America (US, CA, MX)
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

BASE_TABLE_BASH_TEMPLATE = """bq query --nouse_legacy_sql --project_id={{ params.project_id }}  'CREATE OR REPLACE TABLE `{{ params.curated_table }}` AS
    SELECT * FROM (
    SELECT
        demand.ord_dt_key_no AS ord_dt_key_no,
        demand.ord_id AS ord_id,
        demand.global_account_id AS global_account_id,
        demand.oper_cntry_id AS oper_cntry_id,
        demand.ord_account_id AS demand_ord_account_id,
        ord.bus_natr_cd AS ord_bus_natr_cd,
        ship.bus_natr_cd AS ship_bus_natr_cd,
        demand.ord_item_cd AS ord_item_cd,
        demand.ord_item_base_cd,
        demand.ord_qty AS ord_qty,
        demand.catalog_ln_lc_net AS catalog_ln_lc_net,
        demand.adj_ln_lc_net AS adj_ln_lc_net,
        country.country_short_name AS country_name,
        demand.ord_mo_yr_id AS ord_mo_yr_id,
        demand.catalog_ln_usd_net AS catalog_ln_usd_net,
        demand.prm_ln_usd_net AS prm_ln_usd_net,
        demand.cpn_ln_usd_net AS cpn_ln_usd_net,
        demand.adj_ln_usd_net AS adj_ln_usd_net,
        demand.account_type_ord
    FROM ( (
        SELECT
            ord_mo_yr_id,
            ord_dt_key_no,
            ord_id,
            global_account_id,
            ord_party_id,
            oper_cntry_id,
            ord_account_id,
            shp_account_id,
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
            `amw-dna-coe-curated.demand.ds_det_daily_v` ) ) AS demand
    LEFT OUTER JOIN ( (
        SELECT
          country_id as cntry_id,
          account_id,
          business_nature_cd as bus_natr_cd
        FROM
            `amw-dna-coe-curated.account.account_master` ) ) AS ord
    ON
        demand.oper_cntry_id = ord.cntry_id
        AND demand.ord_account_id = ord.account_id
    LEFT OUTER JOIN ( (
        SELECT
            cntry_id,
            account_id,
            bus_natr_cd
        FROM
            `amw-dna-coe-curated.account_master.account_master_flatten` ) ) AS ship
    ON
        demand.oper_cntry_id = ord.cntry_id
        AND demand.shp_account_id = ship.account_id
    INNER JOIN ( (
        SELECT
            amway_country_no,
            country_short_name
        FROM
            `amw-dna-coe-curated.common.countries` ) ) AS country
    ON
        demand.oper_cntry_id = country.amway_country_no )
    WHERE
    ord_mo_yr_id >= CAST( FORMAT_DATE( "%Y%m", DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) ) AS INTEGER )' """

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

with DAG('ds_apr_order_load_North_America_v1',
         description='Workflow to load order data into gcs for APR - North America - US, CA',
         schedule_interval="0 8 * * 1",
         #schedule_interval="@once",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         #start_date=days_ago(1),
         start_date=datetime(2021,11,1),
         catchup=False) as dag:

    START = DummyOperator(task_id="start")
    END = DummyOperator(task_id="end")
    COUNTRY_LIST = pd.read_csv(StringIO(Variable.get('DS_APR_HEAP_NORTH_AMERICA')), header='infer')

    build_base_table = BashOperator(
        task_id='build_base_table',
        bash_command=BASE_TABLE_BASH_TEMPLATE,
        params={
            "curated_table":DAG_CONFIG['CURATED_TABLE'],
            "project_id":DAG_CONFIG["PROJECT_ID"]
        }
    )
    START >> build_base_table

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
        START >> build_base_table >> gcs_delete_order_data >> gcs_load_order_data >> END
