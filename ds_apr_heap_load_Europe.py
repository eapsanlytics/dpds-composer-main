"""
DS APR:  Load heap data into GCS for APR - UK
"""
from datetime import datetime, timedelta, date
from io import StringIO
import time

import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.utils.dates import days_ago
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from ingestion.alerts.custom_ms_teams_webhook_operator import CustomMSTeamsWebhookOperator

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

DAG_CONFIG = Variable.get('DS_APR_PERSONAL_CONFIG', deserialize_json=True)

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

with DAG('ds_apr_heap_load_Europe_v1',
         description='Workflow to load heap data into GCS for APR - UK',
         schedule_interval="0 8 * * 1",
         #schedule_interval="@once",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         #start_date=days_ago(1),
         start_date=datetime(2022,3,7),
         catchup=False) as dag:

    START = DummyOperator(task_id="start")
    END = DummyOperator(task_id="end")
    COUNTRY_LIST = pd.read_csv(StringIO(Variable.get('DS_APR_HEAP_EUROPE')), header='infer')

    VIEW_DELETE_TEMPLATE = """gsutil -m rm -r gs://{{params.gcs_bucket}}/{{params.folder}}/browse/pdp-view/"""
    CART_DELETE_TEMPLATE = """gsutil -m rm -r gs://{{params.gcs_bucket}}/{{params.folder}}/browse/add-to-cart/"""
    ORDER_DELETE_TEMPLATE = """gsutil -m rm -r gs://{{params.gcs_bucket}}/{{params.folder}}/browse/submit-orders/"""
    SEARCH_VIEW_DELETE_TEMPLATE = """gsutil -m rm -r gs://{{params.gcs_bucket}}/{{params.folder}}/browse/search-view/"""
    
    PDP_VIEW_IDENTITY_TEMPLATE = """bq query --nouse_legacy_sql --project_id={{params.project}} "EXPORT DATA OPTIONS(
            uri='gs://{{params.gcs_bucket}}/{{params.folder}}/browse/pdp-view/*.csv',
            format='CSV',
            overwrite=true,
            header=true,
            field_delimiter=';') AS
        WITH DATA AS (
        SELECT
            user_id,
            session_id,
            DATE(time) AS day,
            product_id,
            'product_view_pdp' AS event,
            COUNT(*) AS count
        FROM
            heap_ecomm_{{params.data_topic}}_production_migrated.product_view_pdp
        WHERE
            domain = '{{params.url}}'
            AND DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH) AND CURRENT_DATE()
        GROUP BY
            1,2,3,4,5 ),
        users AS (
        SELECT DISTINCT
            user_id,
            identity
        FROM
            heap_ecomm_{{params.data_topic}}_production_migrated.users )
        SELECT
            day,
            user_id,
            identity,
            product_id,
            session_id,
            SUM(CASE WHEN event = 'product_view_pdp' THEN count END ) AS product_view_pdp
        FROM
            DATA
        LEFT JOIN
            users
        USING
            (user_id)
        GROUP BY
        1,2,3,4,5" """

    ADD_TO_CART_IDENTITY_TEMPLATE = """bq query --nouse_legacy_sql --project_id={{params.project}} "EXPORT DATA
        OPTIONS( uri='gs://{{params.gcs_bucket}}/{{params.folder}}/browse/add-to-cart/*.csv',
            format='CSV',
            overwrite=TRUE,
            header=TRUE,
            field_delimiter=';') AS
        WITH
        DATA AS (
        SELECT
            user_id,
            session_id,
            DATE(time) AS day,
            product_id,
            'added_to_cart' as event,
            COUNT(*) AS count
        FROM
            heap_ecomm_{{params.data_topic}}_production_migrated.product_click_add_to_cart
        WHERE
            domain = '{{params.url}}'
            AND DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
            AND CURRENT_DATE()
        GROUP BY
            1,2,3,4,5),
        users AS (
        SELECT DISTINCT
            user_id,
            identity
        FROM
            heap_ecomm_{{params.data_topic}}_production_migrated.users )
        SELECT
            day,
            user_id,
            identity,
            product_id,
            session_id,
            SUM(CASE WHEN event = 'added_to_cart' THEN count END ) AS added_to_cart
        FROM
            DATA
        LEFT JOIN
            users
        USING
            (user_id)
        GROUP BY
            1,2,3,4,5" """

    SUBMIT_ORDER_IDENTITY_TEMPLATE = """bq query --nouse_legacy_sql --project_id={{params.project}} "EXPORT DATA
        OPTIONS( uri='gs://{{params.gcs_bucket}}/{{params.folder}}/browse/submit-orders/*.csv',
            format='CSV',
            overwrite=TRUE,
            header=TRUE,
            field_delimiter=';') AS
        WITH
        DATA AS (
        SELECT
            user_id,
            session_id,
            DATE(time) AS day,
            product_id,
            'orders_submitted' as event,
            COUNT(*) AS count
        FROM
            heap_ecomm_{{params.data_topic}}_production_migrated.product_click_submit_order
        WHERE
            domain = '{{params.url}}'
            AND DATE(time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
            AND CURRENT_DATE()
        GROUP BY
            1,2,3,4,5 ),
        users AS (
        SELECT DISTINCT
            user_id,
            identity
        FROM
            heap_ecomm_{{params.data_topic}}_production_migrated.users )
        SELECT
            day,
            user_id,
            identity,
            product_id,
            session_id,
            SUM(CASE WHEN event = 'orders_submitted' THEN count END ) AS orders_submitted
        FROM
            DATA
        LEFT JOIN
            users
        USING
            (user_id)
        GROUP BY
            1,2,3,4,5" """

    SEARCH_VIEW_IDENTITY_TEMPLATE = """bq query --nouse_legacy_sql --project_id={{params.project}} "EXPORT DATA OPTIONS(
            uri='gs://{{params.gcs_bucket}}/{{params.folder}}/browse/search-view/*.csv',
            format='CSV',
            overwrite=true,
            header=true,
            field_delimiter=';') AS
        SELECT
            user_id,
            session_id,
            DATE(time) AS day,
            searchkeyword
        FROM
            amw-dna-ingestion-prd.heap_ecomm_{{params.data_topic}}_production_migrated.search_view_results AS SEARCH
        WHERE
            DATE(search.time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
            AND CURRENT_DATE()
            AND searchkeyword IS NOT NULL
            AND domain = '{{params.url}}'
        GROUP BY
            1, 2, 3, 4" """
            
    #pylint: disable=pointless-statement
    START
    for index, row in COUNTRY_LIST.iterrows():
        gcs_delete_view_data = BashOperator(
            task_id=f"gcs_delete_{row['country']}_view_data",
            bash_command=VIEW_DELETE_TEMPLATE,
            params={
                "country":row["country"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        run_pdp_view_identity_command = BashOperator(
            task_id=f"heap_pdp_view_identity_{row['country']}",
            bash_command=PDP_VIEW_IDENTITY_TEMPLATE,
            params={
                "country":row["country"],
                "data_topic":row["data_topic"],
                "url":row["url"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        gcs_delete_cart_data = BashOperator(
            task_id=f"gcs_delete_{row['country']}_cart_data",
            bash_command=CART_DELETE_TEMPLATE,
            params={
                "country":row["country"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        run_cart_identity_command = BashOperator(
            task_id=f"heap_cart_identity_{row['country']}",
            bash_command=ADD_TO_CART_IDENTITY_TEMPLATE,
            params={
                "country":row["country"],
                "data_topic":row["data_topic"],
                "url":row["url"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        gcs_delete_order_data = BashOperator(
            task_id=f"gcs_delete_{row['country']}_order_data",
            bash_command=ORDER_DELETE_TEMPLATE,
            params={
                "country":row["country"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        run_order_identity_command = BashOperator(
            task_id=f"heap_order_identity_{row['country']}",
            bash_command=SUBMIT_ORDER_IDENTITY_TEMPLATE,
            params={
                "country":row["country"],
                "data_topic":row["data_topic"],
                "url":row["url"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        gcs_delete_search_view_data = BashOperator(
            task_id=f"gcs_delete_{row['country']}_search_view_data",
            bash_command=SEARCH_VIEW_DELETE_TEMPLATE,
            params={
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        run_search_view_identity_command = BashOperator(
            task_id=f"heap_search_view_identity_{row['country']}",
            bash_command=SEARCH_VIEW_IDENTITY_TEMPLATE,
            params={
                "data_topic":row["data_topic"],
                "url":row["url"],
                "folder":row['folder'],
                "gcs_bucket":row['gcs_bucket'],
                "project":DAG_CONFIG["PROJECT"]
            }
        )
        #pylint: disable=pointless-statement
        START >> gcs_delete_view_data >> run_pdp_view_identity_command >> gcs_delete_cart_data >> run_cart_identity_command >> gcs_delete_order_data >> run_order_identity_command >> gcs_delete_search_view_data >> run_search_view_identity_command >> END
