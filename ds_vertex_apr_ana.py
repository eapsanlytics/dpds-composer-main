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

DAG_CONFIG = Variable.get('DS_APR_VERTEX_ANA_CONFIG', deserialize_json=True)

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

MARKET_LIST = DAG_CONFIG['MARKET_LIST'].split(',')

with DAG('ds_vertex_apr_ana_v1',
         description='Workflow to run the APR pipeline definition for ANA markets',
         tags=['apr','vertex','kubeflow'],
         schedule_interval="@once",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=datetime(2023,10,1),
         catchup=False) as dag:

    START = EmptyOperator(task_id="start")
    END = EmptyOperator(task_id="end")
    
    for market in MARKET_LIST:  
        RUN_PIPELINE = KubernetesPodOperator(
            task_id=f"run-apr-container-{market}",
            name="run-apr-container",
            # namespace="ml-ops",
            namespace="composer-user-workloads",
            # service_account_name="sa-ml-ops",
            startup_timeout_seconds=600,
            #in_cluster=True,
            image=DAG_CONFIG['DOCKER_IMAGE'],
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
                "REGION": DAG_CONFIG['REGION'],
                "PROJECT_ID": DAG_CONFIG['PROJECT_ID'],
                "DATA_BUCKET": DAG_CONFIG['DATA_BUCKET'],
                "STAGING_BUCKET": DAG_CONFIG['STAGING_BUCKET'],
                "RECOMMENDATION_BUCKET": DAG_CONFIG['RECOMMENDATION_BUCKET'],
                "PIPELINE_ROOT": DAG_CONFIG['PIPELINE_ROOT'],
                "LOCATION": DAG_CONFIG['REGION'],
                "ZONE": DAG_CONFIG['REGION'],
                "ITEM_MAX": DAG_CONFIG['ITEM_MAX'],
                "ITEM_MIN": DAG_CONFIG['ITEM_MIN'],
                "SUBNETWORK_URI": DAG_CONFIG['SUBNETWORK_URI'],
                "JAR_FILES": DAG_CONFIG['JAR_FILES'],
                "MARKET": market,
                "SPARK_IMAGE": DAG_CONFIG['SPARK_IMAGE'],
                "SERVICE_ACCOUNT": DAG_CONFIG['SERVICE_ACCOUNT']
            },
            # Sets the config file to a kubernetes config file specified in
            # airflow.cfg. If the configuration file does not exist or does
            # not provide validcredentials the pod will fail to launch. If not
            # specified, config_file defaults to ~/.kube/config
            config_file="{{ conf.get('kubernetes', 'config_file') }}",
            dag=dag
        )
        START >> RUN_PIPELINE
    RUN_PIPELINE  >> END