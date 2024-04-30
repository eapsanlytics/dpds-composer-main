"""
DS Smart Assistant six to fifteen migration model pipeline
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
from airflow.operators.python_operator import (BranchPythonOperator,
                                               PythonOperator)
from airflow.utils.dates import days_ago
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

from ingestion.alerts.custom_ms_teams_webhook_operator import \
    CustomMSTeamsWebhookOperator

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

DAG_CONFIG = Variable.get('DS_SA_SIX_FIFTEEN_CONFIG', deserialize_json=True)

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

def get_instance_ip(ti,**kwargs):
    """
    gather a dynamically generated instance's IP address
    """
    credentials = GoogleCredentials.get_application_default()
    service = discovery.build('compute', 'v1', credentials=credentials)

    project = DAG_CONFIG["PROJECT"]
    zone = DAG_CONFIG["ZONE"]
    instance = DAG_CONFIG["INSTANCE_NAME"]

    #pylint: disable=pointless-statement
    request = service.instances().get(project=project, zone=zone, instance=instance)
    response = request.execute()
    instance_ip = response['networkInterfaces'][0]['networkIP']
    ti.xcom_push(key='recent_ip', value=instance_ip)
    return instance_ip

def pause_for_provisioning(**kwards):
    """
    creates a 3 minute window to allow for the start up scripts to complete their work
    """
    time.sleep(240)
    return 0

today = date.today()
TIME_STAMP = today.strftime("%b-%d-%Y")

CREATE_E2_INSTANCE_BASH_TEMPLATE = Variable.get('CREATE_E2_INSTANCE_BASH_TEMPLATE')
GET_E2_INSTANCE_IP = Variable.get('DS_E2_MIGRATION_GET_IP')
DELETE_E2_INSTANCE_BASH_TEMPLATE = Variable.get('DELETE_E2_INSTANCE_BASH_TEMPLATE')
UPDATE_YML = """cp /home/airflow/gcs/data/mj-migration.pem /tmp; chmod 600 /tmp/mj-migration.pem; ssh -i /tmp/mj-migration.pem -o StrictHostKeyChecking=no michael_janzen_amway_com@{{ ti.xcom_pull(key="recent_ip") }} "sudo -- bash -c 'whoami; cd /migration-home; ls -l; pwd; python3 /migration-home/smart_assist_2.0/migration/six_fifteen/update_six_fifteen_yml.py --timestamp """ + TIME_STAMP + """' " """
MIGRATION_BQ = """cp /home/airflow/gcs/data/mj-migration.pem /tmp; chmod 600 /tmp/mj-migration.pem; ssh -i /tmp/mj-migration.pem -o StrictHostKeyChecking=no michael_janzen_amway_com@{{ ti.xcom_pull(key="recent_ip") }} "sudo -- bash -c 'whoami; cd /migration-home; ls -l; pwd; python3 /migration-home/smart_assist_2.0/migration/six_fifteen/BQ-migration-6to15.py --timestamp """ + TIME_STAMP + """' " """
MIGRATION_TEST_SET = """cp /home/airflow/gcs/data/mj-migration.pem /tmp; chmod 600 /tmp/mj-migration.pem; ssh -i /tmp/mj-migration.pem -o StrictHostKeyChecking=no michael_janzen_amway_com@{{ ti.xcom_pull(key="recent_ip") }} "sudo -- bash -c 'whoami; cd /migration-home; ls -l; pwd; python3 /migration-home/smart_assist_2.0/migration/six_fifteen/BQ-migration-6to15-Test-Set.py --timestamp """ + TIME_STAMP + """' " """
MIGRATION_GET_DATA = """cp /home/airflow/gcs/data/mj-migration.pem /tmp; chmod 600 /tmp/mj-migration.pem; ssh -i /tmp/mj-migration.pem -o StrictHostKeyChecking=no michael_janzen_amway_com@{{ ti.xcom_pull(key="recent_ip") }} "sudo -- bash -c 'whoami; cd /migration-home; ls -l; pwd; python3 /migration-home/smart_assist_2.0/migration/six_fifteen/get_data.py --timestamp """ + TIME_STAMP + """' " """
MIGRATION_TRAIN = """cp /home/airflow/gcs/data/mj-migration.pem /tmp; chmod 600 /tmp/mj-migration.pem; ssh -i /tmp/mj-migration.pem -o StrictHostKeyChecking=no michael_janzen_amway_com@{{ ti.xcom_pull(key="recent_ip") }} "sudo -- bash -c 'whoami; cd /migration-home; ls -l; pwd; python3 /migration-home/smart_assist_2.0/migration/six_fifteen/train_validate_update.py --timestamp """ + TIME_STAMP + """ --action t ' " """
MIGRATION_SCORE = """cp /home/airflow/gcs/data/mj-migration.pem /tmp; chmod 600 /tmp/mj-migration.pem; ssh -i /tmp/mj-migration.pem -o StrictHostKeyChecking=no michael_janzen_amway_com@{{ ti.xcom_pull(key="recent_ip") }} "sudo -- bash -c 'whoami; cd /migration-home; ls -l; pwd; python3 /migration-home/smart_assist_2.0/migration/six_fifteen/train_validate_update.py --timestamp """ + TIME_STAMP + """ --action s ' " """
FINAL_CURATION = """ssh -i /tmp/mj-migration.pem -o StrictHostKeyChecking=no michael_janzen_amway_com@{{ ti.xcom_pull(key="recent_ip") }} "sudo -- bash -c 'whoami; cd /migration-home; ls -l; pwd; python3 /migration-home/smart_assist_2.0/migration/six_fifteen/six_fifteen_final_curation.py --timestamp """ + TIME_STAMP + """ ' " """

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

with DAG('ds_sa_six_fifteen_v1',
         description='Workflow to run migration models in GCP',
         schedule_interval="10 2 5,11 * *",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=datetime(2022,4,1),
         catchup=False) as dag:

    START = DummyOperator(task_id="start")
    END = DummyOperator(task_id="end")

    create_e2_instance = BashOperator(
        task_id="create_e2_instance",
        bash_command=CREATE_E2_INSTANCE_BASH_TEMPLATE,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "machine_type":DAG_CONFIG["MACHINE_TYPE"],
	        "scopes":DAG_CONFIG["SCOPES"],
            "subnet":DAG_CONFIG["SUBNET"],
            "project":DAG_CONFIG["PROJECT"],
			"boot_disk_size":DAG_CONFIG["BOOT_DISK_SIZE"],
            "startup_script":DAG_CONFIG["STARTUP_SCRIPT"],
            "service_account":DAG_CONFIG["SERVICE_ACCOUNT"]
        }
    )
    provision_pause = PythonOperator(
        task_id='pause_for_provisioning',
        python_callable=pause_for_provisioning,
        provide_context=True
    )
    ipaddy = e2_migration_get_ip = PythonOperator(
        task_id='get_instance_ip',
        python_callable=get_instance_ip,
        provide_context=True,
        do_xcom_push=True
    )
    update_yml = BashOperator(
        task_id="update_yml",
        bash_command=UPDATE_YML,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    e2_migration_bq = BashOperator(
        task_id="e2_migration_bq",
        bash_command=MIGRATION_BQ,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    e2_migration_test_set = BashOperator(
        task_id="e2_migration_test_set",
        bash_command=MIGRATION_TEST_SET,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    e2_migration_get_data = BashOperator(
        task_id="e2_migration_get_data",
        bash_command=MIGRATION_GET_DATA,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    e2_migration_train = BashOperator(
        task_id="e2_migration_train",
        bash_command=MIGRATION_TRAIN,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    e2_migration_score = BashOperator(
        task_id="e2_migration_score",
        bash_command=MIGRATION_SCORE,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    final_curation = BashOperator(
        task_id="final_curation",
        bash_command=FINAL_CURATION,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    delete_e2_instance = BashOperator(
        task_id="delete_e2_instance",
        bash_command=DELETE_E2_INSTANCE_BASH_TEMPLATE,
        params={
            "instance_name":DAG_CONFIG["INSTANCE_NAME"],
            "zone":DAG_CONFIG["ZONE"],
            "project":DAG_CONFIG["PROJECT"]
        }
    )
    #pylint: disable=pointless-statement
    START >> create_e2_instance >> provision_pause >> e2_migration_get_ip >> update_yml >> e2_migration_bq >> e2_migration_test_set >> e2_migration_get_data >> e2_migration_train >> e2_migration_score >> final_curation >> delete_e2_instance >>END
