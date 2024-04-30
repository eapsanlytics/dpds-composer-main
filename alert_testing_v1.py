"""
Test MS Teams alerting mechanism
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from ingestion.alerts.custom_ms_teams_webhook_operator import (
    CustomMSTeamsWebhookOperator,
)

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"


def send_teams_notification(context):
    """
    Sends a Microsoft Teams notification using a custom webhook.

    Args:
        context (dict): The context dictionary containing the task execution context.

    Returns:
        None
    """
    teams_notification = CustomMSTeamsWebhookOperator(
        task_id="send_teams_notification",
        http_conn_id=MSTEAMS_ALERT_CONN_ID,
        context=context,
    )
    teams_notification.execute(context)


def test_alert():
    """
    Test the alerting mechanism
    """
    raise Exception("Test exception")


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "weight_rule": "upstream",
    "on_retry_callback": send_teams_notification,
    "on_failure_callback": send_teams_notification,
}

with DAG(
    "alert_testing_v1",
    description="Workflow for testing the MS Teams alerting mechanism",
    schedule_interval="@once",
    default_args=DEFAULT_ARGS,
    concurrency=12,
    max_active_runs=1,
    start_date=datetime(2024, 4, 1),
    catchup=False,
) as dag:

    START = EmptyOperator(task_id="start")
    END = EmptyOperator(task_id="end")

    TEST_FAILURE_ALERT = PythonOperator(
        task_id="python_task",
        python_callable=test_alert,
        dag=dag,
    )

    TEST_SUCCESS_NOTICE = CustomMSTeamsWebhookOperator(
        task_id="task_success",
        http_conn_id=MSTEAMS_ALERT_CONN_ID,
        message="Pipeline completed successfully",
        subtitle="Pipeline completed successfully",
        trigger_rule="all_done",
        dag=dag,
    )

    # pylint: disable=pointless-statement
    START >> TEST_FAILURE_ALERT >> TEST_SUCCESS_NOTICE >> END
