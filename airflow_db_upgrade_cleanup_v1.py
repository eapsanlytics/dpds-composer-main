"""
Remove upgrade artifacts from the airflow db
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['shane.graham@amway.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG('airflow_db_upgrade_cleanup_v1',
         description='Remove upgrade artifacts from the airflow db',
         schedule_interval='@once',
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=datetime(2022,11,15),
         catchup=False) as dag:

    START = DummyOperator(task_id="start")

    DB_OP1 = PostgresOperator(
        task_id="db_op1",
        sql="drop table _airflow_moved__2_3__dangling__rendered_task_instance_fields",
        postgres_conn_id='airflow_db'
    )

    DB_OP2 = PostgresOperator(
        task_id="db_op2",
        sql="drop table _airflow_moved__2_3__dangling__task_fail",
        postgres_conn_id='airflow_db'
    )

    END = DummyOperator(task_id="end")

    START >> DB_OP1 >> DB_OP2 >> END