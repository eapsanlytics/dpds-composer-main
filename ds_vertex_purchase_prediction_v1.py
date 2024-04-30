"""
DS VERTEX PURCHASE PREDICTION:  Run the Purchase Prediction pipeline
"""
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

DAG_CONFIG = Variable.get('DS_VERTEX_PURCHASE_PREDICTION_CONFIG', deserialize_json=True)

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

with DAG('ds_vertex_purchase_prediction_v1',
         description='Workflow to run the Purchase Prediction Vertex pipeline',
         tags=['purchase_prediction','vertex'],
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
        task_id="run-pp-container",
        name="run-pp-container",
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
            "PP_PROJECT_ID": DAG_CONFIG['PP_PROJECT_ID'],
            "PP_REGION": DAG_CONFIG['PP_REGION'],
            "PP_MARKET_LIST": DAG_CONFIG['PP_MARKET_LIST'],
            "PP_INPUT_TABLESPEC": DAG_CONFIG['PP_INPUT_TABLESPEC'],
            "PP_BUCKET_NAME": DAG_CONFIG['PP_BUCKET_NAME'],
            "PP_MONTH_START": DAG_CONFIG['PP_MONTH_START'],
            "PP_PRED_START_DATE":  '{{ (dag_run.logical_date + macros.dateutil.relativedelta.relativedelta(months=-2)).strftime("%Y-%m-%d") }}',
            "PP_MIN_TARGET_MONTH": '{{ (dag_run.logical_date + macros.dateutil.relativedelta.relativedelta(months=-4)).strftime("%Y-%m-%d") }}',
            "PP_MAX_TARGET_MONTH": '{{ (dag_run.logical_date + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y-%m-%d") }}',
            "PP_BQ_PROJECT_ID": DAG_CONFIG['PP_BQ_PROJECT_ID'],
            "PP_TARGET_TABLESPEC": DAG_CONFIG['PP_TARGET_TABLESPEC'],
            "PP_PIPELINE_TS": '{{ macros.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") }}',
            "PP_MODEL_VERSION": DAG_CONFIG['PP_MODEL_VERSION'],
        },
        # Sets the config file to a kubernetes config file specified in
        # airflow.cfg. If the configuration file does not exist or does
        # not provide validcredentials the pod will fail to launch. If not
        # specified, config_file defaults to ~/.kube/config
        config_file="{{ conf.get('kubernetes', 'config_file') }}",
        dag=dag
    )
    
    RUN_CUSTOMER_PIPELINE = KubernetesPodOperator(
        task_id="run-customer-pp-container",
        name="run-customer-pp-container",
        # namespace="ml-ops",
        namespace="composer-user-workloads",
        # service_account_name="sa-ml-ops",
        startup_timeout_seconds=600,
        #in_cluster=True,
        image=DAG_CONFIG['CUSTOMER_PP_IMAGE_URL'],
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
            "PP_PROJECT_ID": DAG_CONFIG['PP_PROJECT_ID'],
            "PP_REGION": DAG_CONFIG['PP_REGION'],
            "PP_MARKET_LIST": DAG_CONFIG['PP_MARKET_LIST'],
            "PP_INPUT_TABLESPEC": DAG_CONFIG['PP_INPUT_TABLESPEC'],
            "PP_BUCKET_NAME": DAG_CONFIG['PP_BUCKET_NAME'],
            "PP_MONTH_START": DAG_CONFIG['PP_MONTH_START'],
            "PP_PRED_START_DATE":  '{{ (dag_run.logical_date + macros.dateutil.relativedelta.relativedelta(months=-2)).strftime("%Y-%m-%d") }}',
            "PP_MIN_TARGET_MONTH": '{{ (dag_run.logical_date + macros.dateutil.relativedelta.relativedelta(months=-4)).strftime("%Y-%m-%d") }}',
            "PP_MAX_TARGET_MONTH": '{{ (dag_run.logical_date + macros.dateutil.relativedelta.relativedelta(months=1, days=-1)).strftime("%Y-%m-%d") }}',
            "PP_BQ_PROJECT_ID": DAG_CONFIG['PP_BQ_PROJECT_ID'],
            "PP_TARGET_TABLESPEC": DAG_CONFIG['PP_CUSTOMER_TARGET_TABLESPEC'],
            "PP_PIPELINE_TS": '{{ macros.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") }}',
            "PP_MODEL_VERSION": DAG_CONFIG['PP_MODEL_VERSION'],
        },
        # Sets the config file to a kubernetes config file specified in
        # airflow.cfg. If the configuration file does not exist or does
        # not provide validcredentials the pod will fail to launch. If not
        # specified, config_file defaults to ~/.kube/config
        config_file="{{ conf.get('kubernetes', 'config_file') }}",
        dag=dag
    )

    START >> WAIT_FOR_FEATURE_DAG >> [ RUN_PIPELINE, RUN_CUSTOMER_PIPELINE ] >> END
