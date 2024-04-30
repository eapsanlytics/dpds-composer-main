"""
DS VERTEX MACRO FORECAST:  Run the Macro Forecast pipeline
"""
import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Connection
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client.models import V1ResourceRequirements

from ingestion.alerts.custom_ms_teams_webhook_operator import CustomMSTeamsWebhookOperator


MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

#DAG_CONFIG = Variable.get('COMPLIANCE_COMPANION_PIPELINE_CONFIG', deserialize_json=True)

AIRFLOW_ENV = str(os.environ.get('AIRFLOW_ENV', 'DEV')).lower()
DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
# Read the config json file into the DAG_CONFIG dictionary
with open(f'{DAGS_DIR}/config/{AIRFLOW_ENV}/COMPLIANCE_COMPANION_CONFIG.json', 'r', encoding='UTF-8') as f:
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

def get_matillion_credentials():
    """
    Retrieves the username and password from the Airflow connection for Matillion.

    Returns:
        tuple: A tuple containing the username and password.
    """
    conn_id = DAG_CONFIG['MATILLION_CONNECTION_NAME']
    conn = Connection.get_connection_from_secrets(conn_id)
    username = conn.login
    password = conn.password
    server_url = conn.host
    return username, password, server_url

def run_matillion_job():
    """
    Retrieves a bearer token from Azure Active Directory using the provided client ID, client secret, and tenant ID.

    Args:
        client_id (str): The client ID of the Azure AD application.
        client_secret (str): The client secret of the Azure AD application.
        tenant_id (str): The tenant ID of the Azure AD directory.

    Returns:
        str: The bearer token retrieved from Azure AD.
    """
    # pylint: disable=import-outside-toplevel
    import requests
    import base64

    group_name = DAG_CONFIG['MATILLION_GROUP_NAME']
    project_name = DAG_CONFIG['MATILLION_PROJECT_NAME']
    version_name = DAG_CONFIG['MATILLION_VERSION_NAME']
    job_name = DAG_CONFIG['MATILLION_JOB_NAME']
    environment_name = DAG_CONFIG['MATILLION_ENVIRONMENT_NAME']
    username, password, server_url = get_matillion_credentials()

    url = f"https://{server_url}/rest/v1/group/name/{group_name}/project/name/{project_name}/version/name/{version_name}/job/name/{job_name}/run?environmentName={environment_name}"


    response = requests.post(url, auth=(username, password), timeout=30, verify=False)
    response.raise_for_status()  # ensure we notice bad responses

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'weight_rule': 'upstream',
    'on_retry_callback': msteams_task_retry_alert,
    'on_failure_callback': msteams_task_fail_alert
}

with DAG('compliance_companion_pipeline_v1',
         description='Workflow to run the ingestion and ML pipelines for the Compliance Companion project.',
         tags=['compliance_companion'],
         schedule_interval="0 6 * * *",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=datetime(2023,11,26),
         catchup=False) as dag:

    START = EmptyOperator(task_id="start")
    END = EmptyOperator(task_id="end")

    SHAREPOINT_SYNC = KubernetesPodOperator(
        task_id="run-sharpoint-sync",
        name="run-sharpoint-sync",
        # namespace="ml-ops",
        namespace="composer-user-workloads",
        # service_account_name="sa-ml-ops",
        startup_timeout_seconds=600,
        #in_cluster=True,
        image=DAG_CONFIG['SHAREPOINT_SYNC_IMAGE_URL'],
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
            "PROJECT_ID": DAG_CONFIG['PROJECT_ID'],
            "SECRET_ID": DAG_CONFIG['SECRET_ID'],
            "NUM_WORKERS": DAG_CONFIG['NUM_WORKERS'],
            "BUCKET_NAME": DAG_CONFIG['BUCKET_NAME'],
        },
        # Sets the config file to a kubernetes config file specified in
        # airflow.cfg. If the configuration file does not exist or does
        # not provide validcredentials the pod will fail to launch. If not
        # specified, config_file defaults to ~/.kube/config
        config_file="{{ conf.get('kubernetes_executor', 'config_file') }}",
        dag=dag,
        retries=0,
        container_resources=V1ResourceRequirements(
            requests={
                "cpu": "4000m",
                "ephemeral-storage": "8Gi",
                "memory": "8Gi"
            },
            limits={
                "cpu": "4000m",
                "ephemeral-storage": "8Gi",
                "memory": "8Gi"
            }
        )
    )

    MATILLION_JOB = PythonOperator(
        task_id='run-matillion-job',
        python_callable=run_matillion_job,
        dag=dag
    )

    ML_PIPELINE = KubernetesPodOperator(
        task_id="run-ml-pipeline",
        name="run-ml-pipeline",
        # namespace="ml-ops",
        namespace="composer-user-workloads",
        # service_account_name="sa-ml-ops",
        startup_timeout_seconds=600,
        #in_cluster=True,
        image=DAG_CONFIG['ML_PIPELINE_IMAGE_URL'],
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
            "CC_PROJECT_ID": DAG_CONFIG['CC_PROJECT_ID'],
            "CC_PIPELINE_BUCKET_NAME": DAG_CONFIG['CC_PIPELINE_BUCKET_NAME'],
            "CC_REGION": DAG_CONFIG['CC_REGION'],
            "CC_INPUT_BUCKET": DAG_CONFIG['CC_INPUT_BUCKET'],
            "CC_OUTPUT_BUCKET": DAG_CONFIG['CC_OUTPUT_BUCKET'],
            "CC_INPUT_DATASET": DAG_CONFIG['CC_INPUT_DATASET'],
            "CC_INPUT_TABLE": DAG_CONFIG['CC_INPUT_TABLE'],
            "CC_INPUT_DATE": "{{ logical_date.strftime('%Y%m%d') }}",
            "CC_OUTPUT_DATASET": DAG_CONFIG['CC_OUTPUT_DATASET'],
            "CC_OUTPUT_TABLE": DAG_CONFIG['CC_OUTPUT_TABLE'],
            "CC_WHISPER_MODEL_SIZE": DAG_CONFIG['CC_WHISPER_MODEL_SIZE'],
            "CC_RUN_ID": "{{ logical_date.strftime('%Y%m%d') }}",
            "CC_LOOP_DELAY": DAG_CONFIG['CC_LOOP_DELAY'],
        },
        # Sets the config file to a kubernetes config file specified in
        # airflow.cfg. If the configuration file does not exist or does
        # not provide validcredentials the pod will fail to launch. If not
        # specified, config_file defaults to ~/.kube/config
        config_file="{{ conf.get('kubernetes_executor', 'config_file') }}",
        dag=dag,
        retries=0
    )

    #pylint: disable=pointless-statement
    START >> [ SHAREPOINT_SYNC, MATILLION_JOB ] >> ML_PIPELINE >> END
