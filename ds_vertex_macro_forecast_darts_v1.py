"""
DS VERTEX MACRO FORECAST:  Run the Macro Forecast pipeline
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

from ingestion.alerts.custom_ms_teams_webhook_operator import CustomMSTeamsWebhookOperator


MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

DAG_CONFIG = Variable.get('DS_VERTEX_MACRO_FORECAST_DARTS_CONFIG', deserialize_json=True)

DATA_EXISTS_SQL = f"""SELECT COUNT(DISTINCT CNTRY_KEY_NO) COUNTRY_COUNT FROM `{DAG_CONFIG['MF_HYPERION_REVENUE_TABLESPEC']}` """ + \
    """WHERE MO_YR_KEY_NO = '{{ dag_run.logical_date.strftime("%Y%m") }}' HAVING COUNT(DISTINCT CNTRY_KEY_NO) >= """ + \
    str(DAG_CONFIG['MF_COUNTRY_COUNT'])

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

def check_data_exists(sql, gcp_conn_id):
    """
    Check if data exists for the previous month
    """
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = hook.get_client()

    query_job = client.query(sql)
    rows = query_job.result()
    for row in rows:
        if row[0] >= DAG_CONFIG['MF_COUNTRY_COUNT']:
            return True
        else:
            return False

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=15),
    'weight_rule': 'upstream',
    'on_retry_callback': msteams_task_retry_alert,
    'on_failure_callback': msteams_task_fail_alert
}

with DAG('ds_vertex_macro_forecast_darts_v1',
         description='Workflow to run the Macro Forecast DARTS Vertex pipeline',
         tags=['macro_forecast', 'darts', 'vertex'],
         schedule_interval="0 17 9 * *",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=datetime(2023,4,9),
         catchup=False) as dag:

    START = EmptyOperator(task_id="start")
    END = EmptyOperator(task_id="end")

    CHECK_DATA_EXISTS = PythonSensor(
        task_id="check_data_exists",
        python_callable=check_data_exists,
        op_kwargs={
            'sql': DATA_EXISTS_SQL,
            'gcp_conn_id': 'bigquery_default'
        },
        execution_timeout=timedelta(minutes=5),
        mode='reschedule',
        poke_interval=DAG_CONFIG['MF_DATA_CHECK_INTERVAL'],
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
        image=DAG_CONFIG['PL_IMAGE_URL'],
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
            "PL_REGION": DAG_CONFIG['PL_REGION'],
            "PL_PROJECT_ID": DAG_CONFIG['PL_PROJECT_ID'],
            "PL_BUCKET_NAME": DAG_CONFIG['PL_BUCKET_NAME'],
            "MF_HYPERION_REVENUE_TABLESPEC": DAG_CONFIG['MF_HYPERION_REVENUE_TABLESPEC'],
            "MF_COUNTRIES_TABLESPEC": DAG_CONFIG['MF_COUNTRIES_TABLESPEC'],
            "MF_RAW_PRED_TABLESPEC": DAG_CONFIG['MF_RAW_PRED_TABLESPEC'],
            "MF_FINAL_PRED_TABLESPEC": DAG_CONFIG['MF_FINAL_PRED_TABLESPEC'],
            "MF_FLAGS_TABLESPEC": DAG_CONFIG['MF_FLAGS_TABLESPEC'],
            "MF_HDP_TABLESPEC": DAG_CONFIG['MF_HDP_TABLESPEC'],
            "MF_HDP_STACKED_TABLESPEC": DAG_CONFIG['MF_HDP_STACKED_TABLESPEC'],
            "MF_RUN_ID": '{{ macros.datetime.utcnow().strftime("%Y-%m-%d") }}',
            "MF_RUN_YR_MO": '{{ dag_run.logical_date.strftime("%Y%m") }}',
            "MF_NUM_SAMPLES": DAG_CONFIG['MF_NUM_SAMPLES'],
            "MF_ROLL_MIN_WINDOW_LEN": DAG_CONFIG['MF_ROLL_MIN_WINDOW_LEN'],
            "MF_NUM_STDS": DAG_CONFIG['MF_NUM_STDS'],
            "MF_NUM_PAST_MONTHS": DAG_CONFIG['MF_NUM_PAST_MONTHS'],
            "MF_NUM_PAST_MONTHS_REP": DAG_CONFIG['MF_NUM_PAST_MONTHS_REP'],
            "MF_NUM_FUTURE_MONTHS": DAG_CONFIG['MF_NUM_FUTURE_MONTHS'],
            "MF_ELIM_NEGATIVES": DAG_CONFIG['MF_ELIM_NEGATIVES'],
            "MF_GUARDRAIL_TAG": DAG_CONFIG['MF_GUARDRAIL_TAG'],
            "MF_PIPELINE_TS": '{{ macros.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S") }}',
            "MF_MODEL_VERSION": DAG_CONFIG['MF_MODEL_VERSION'],
        },
        # Sets the config file to a kubernetes config file specified in
        # airflow.cfg. If the configuration file does not exist or does
        # not provide validcredentials the pod will fail to launch. If not
        # specified, config_file defaults to ~/.kube/config
        config_file="{{ conf.get('kubernetes', 'config_file') }}",
        dag=dag,
        retries=0
    )

    #pylint: disable=pointless-statement
    START >> CHECK_DATA_EXISTS >> RUN_PIPELINE >> END
