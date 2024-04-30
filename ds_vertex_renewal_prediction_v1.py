"""
DS VERTEX RENEWAL PREDICTION:  Run the Renewal Prediction pipeline
"""
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from airflow.sensors.external_task import ExternalTaskSensor

from ingestion.alerts.custom_ms_teams_webhook_operator import \
    CustomMSTeamsWebhookOperator

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

DAG_CONFIG = Variable.get('DS_VERTEX_RENEWAL_PREDICTION_CONFIG', deserialize_json=True)

AIRFLOW_ENV = str(os.environ.get('AIRFLOW_ENV', 'DEV')).lower()
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
# Read the config json file into the DAG_CONFIG dictionary
with open(f'{DAGS_DIR}/config/{AIRFLOW_ENV}/DS_FEATURE_PREP_CONFIG.json', 'r', encoding='UTF-8') as f:
    FEATURE_CONFIG = json.load(f)

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
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'weight_rule': 'upstream',
    'on_retry_callback': msteams_task_retry_alert,
    'on_failure_callback': msteams_task_fail_alert
}

with DAG('ds_vertex_renewal_prediction_v1',
         description='Workflow to run the Renewal Prediction Vertex pipeline',
         tags=['renewal_prediction','vertex'],
         schedule_interval="0 17 1 * *",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=datetime(2022,11,1),
         catchup=False) as dag:

    START = EmptyOperator(task_id="start")
    END = EmptyOperator(task_id="end")

    WAIT_FOR_FEATURE_DAG = ExternalTaskSensor(
        task_id='wait_for_features',
        external_dag_id='ds_feature_prep_purchase_pred_renewals_v1',
        external_task_id='end',
        start_date=datetime(2023,2,1),
        execution_delta=timedelta(hours=1),
        allowed_states=['success'],
        timeout=86400,
        check_existence=True,
        dag=dag
    )

    RUN_PIPELINE = KubernetesPodOperator(
        task_id="run-container",
        name="run-container",
        # namespace="ml-ops",
        namespace="composer-user-workloads",
        # service_account_name="sa-ml-ops",
        startup_timeout_seconds=600,
        #in_cluster=True,
        image=DAG_CONFIG['IMAGE_URL'],
        # All parameters below are able to be templated with jinja -- cmds,
        # arguments, env_vars, and config_file. For more information visit:
        # https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
        # Entrypoint of the container, if not specified the Docker container's
        # entrypoint is used. The cmds parameter is templated.
        #cmds=["run.sh"],
        # DS in jinja is the execution date as YYYY-MM-DD, this docker image
        # will echo the execution date. Arguments to the entrypoint. The docker
        # image's CMD is used if this is not provided. The arguments parameter
        # is templated.
        #arguments=["{{ ds }}"],
        # The var template variable allows you to access variables defined in
        # Airflow UI. In this case we are getting the value of my_value and
        # setting the environment variable `MY_VALUE`. The pod will fail if
        # `my_value` is not set in the Airflow UI.
        env_vars={
            "RP_PROJECT_ID": DAG_CONFIG['RP_PROJECT_ID'],
            "RP_REGION": DAG_CONFIG['RP_REGION'],
            "RP_MARKET_LIST": DAG_CONFIG['RP_MARKET_LIST'],
            "RP_BUCKET_NAME": DAG_CONFIG['RP_BUCKET_NAME'],
            "RP_BASE_DATE": '{{ (dag_run.logical_date + macros.dateutil.relativedelta.relativedelta(months=1)).replace(day=1).strftime("%Y-%m-%d") }}',
            "RP_MIN_TARGET_MONTH": DAG_CONFIG['RP_MIN_TARGET_MONTH'],
            "RENEWAL_SPONSOR_INFO_TABLESPEC": FEATURE_CONFIG['RENEWAL_SPONSOR_INFO_TABLESPEC'],
            "RFM_TENURE_STAGING_TABLESPEC": FEATURE_CONFIG['RFM_TENURE_STAGING_TABLESPEC'],
            "RFM_DATA_STAGING_TABLESPEC": FEATURE_CONFIG['RFM_DATA_RENEWAL_STAGING_TABLESPEC'],
            "RFM_MONTHLY_BV_PER_ABO_TABLESPEC": FEATURE_CONFIG['RFM_MONTHLY_BV_PER_ABO_TABLESPEC'],
            "RP_TARGET_TABLESPEC": DAG_CONFIG['RP_TARGET_TABLESPEC'],
            "RP_PIPELINE_TS": '{{ macros.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") }}',
            "RP_MODEL_VERSION": DAG_CONFIG['RP_MODEL_VERSION'],
            "RP_ENVIRONMENT": AIRFLOW_ENV,
        },
        # Sets the config file to a kubernetes config file specified in
        # airflow.cfg. If the configuration file does not exist or does
        # not provide validcredentials the pod will fail to launch. If not
        # specified, config_file defaults to ~/.kube/config
        config_file="{{ conf.get('kubernetes', 'config_file') }}",
        dag=dag
    )

    # pylint: disable=pointless-statement
    START >> WAIT_FOR_FEATURE_DAG >> RUN_PIPELINE >> END
