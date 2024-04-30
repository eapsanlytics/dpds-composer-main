"""
DS FEATURE PREPARATION PIPELINE:  Pred data for feature engineering for various models
"""
import json
import os
from datetime import datetime, timedelta

from airflow import DAG, models
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator)
from airflow.providers.google.cloud.sensors.dataform import \
    DataformWorkflowInvocationStateSensor
from custom.sensors.bigquery import BigQuerySqlSensor
from google.cloud.dataform_v1beta1 import WorkflowInvocation
from ingestion.alerts.custom_ms_teams_webhook_operator import \
    CustomMSTeamsWebhookOperator

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

AIRFLOW_ENV = str(os.environ.get('AIRFLOW_ENV', 'DEV')).lower()
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
# Read the config json file into the DAG_CONFIG dictionary
with open(f'{DAGS_DIR}/config/{AIRFLOW_ENV}/DS_FEATURE_PREP_CONFIG.json', 'r', encoding='UTF-8') as f:
    DAG_CONFIG = json.load(f)

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

def get_affiliates(market_list_in='ALL', return_format='string'):
    """
    Return the list of affiliates for the given markets
    """
    if market_list_in == 'ALL':
        markets = [market for markets_list in DAG_CONFIG['MARKET_LISTS'].values() for market in markets_list]
    else:
        markets = DAG_CONFIG['MARKET_LISTS'].get(market_list_in, [])
    markets.sort()

    aff_set = set()
    for market in markets:
        aff_set.update(DAG_CONFIG['MARKET_DICT'][market]['affiliates'])

    aff_list = sorted(aff_set)

    if return_format.lower() == 'list':
        return aff_list
    else:
        affiliates = ",".join(str(aff) for aff in aff_list)
        return f"({affiliates})"

def get_start_date(start_date_name):
    """
    Return the start date in YYYY-MM-DD format to be used in the query
    """
    start_date = str(DAG_CONFIG['DATE_CONSTANTS'][start_date_name])
    start_date = start_date[0:4] + '-' + start_date[4:6] + '-01'
    return start_date

def get_start_month(start_date_name):
    """
    Return the start month to be used in the query
    """
    return DAG_CONFIG['DATE_CONSTANTS'][start_date_name]

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'weight_rule': 'upstream',
    'on_retry_callback': msteams_task_retry_alert,
    'on_failure_callback': msteams_task_fail_alert
}

with DAG('ds_feature_prep_purchase_pred_renewals_v1',
         description='Workflow to prepare data to be engineered for feature creation',
         tags=['feature_prep', 'purchase_prediction', 'renewal_prediction', 'vertex'],
         schedule_interval="0 16 1 * *",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=datetime(2023,9,1),
         user_defined_macros={
             "get_affiliates": get_affiliates,
             "get_start_month": get_start_month,
             "get_start_date": get_start_date,
         },
         catchup=False) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    check_demand_data_exists = BigQuerySqlSensor(
        task_id='check_demand_data_exists',
        gcp_conn_id='google_cloud_default',
        sql='sql/features/demand_sensor.sql',
        poke_interval=DAG_CONFIG['SENSOR_INTERVAL'],
        timeout=86400,
        dag=dag
    )

    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=DAG_CONFIG['DATAFORM_PROJECT_ID'],
        region=DAG_CONFIG['DATAFORM_REGION'],
        repository_id=DAG_CONFIG['DATAFORM_REPOSITORY_ID'],
        compilation_result={
            "git_commitish": DAG_CONFIG['DATAFORM_GIT_COMMITISH'],
            "code_compilation_config": { "default_database": DAG_CONFIG['DATAFORM_DEFAULT_DATABASE'] },
        },
    )

    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=DAG_CONFIG['DATAFORM_PROJECT_ID'],
        region=DAG_CONFIG['DATAFORM_REGION'],
        repository_id=DAG_CONFIG['DATAFORM_REPOSITORY_ID'],
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": { "included_tags": DAG_CONFIG['DATAFORM_RENEWALS_TAGS'] }
        }
    )

    start >> check_demand_data_exists >> create_compilation_result >> create_workflow_invocation >> end