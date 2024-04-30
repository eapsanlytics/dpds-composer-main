"""
GCP Schema Sync
Backs up meta data for BQ Tables in a given project
"""
from datetime import datetime, timedelta
from io import StringIO

from google.cloud import bigquery
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from ingestion.alerts.custom_ms_teams_webhook_operator import \
    CustomMSTeamsWebhookOperator

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"

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

def backup_table_metadata(**kwargs):
    """
    backs up table schema meta data
    """
    client = bigquery.Client('amw-dna-ingestion-prd')
    datasets = list(client.list_datasets('amw-dna-ingestion-prd'))
    project = 'amw-dna-ingestion-prd'

    tablecnt = 0

    # loop datasets in a given project to backup table meta data
    # from each of those tables.
    if datasets:
        print("Datasets in project {}:".format(project))
        for dataset in datasets:
            # exclude a few we know do not need to be backed up, included those for ingestion
            if dataset.dataset_id[0:8] == "incoming" or dataset.dataset_id[0:9] == "streaming" or dataset.dataset_id[0:7] == "staging" or dataset.dataset_id[0:4] == "temp":
                continue

            print("working on dataset: {}".format(dataset.dataset_id))
            insert = """INSERT INTO `amw-dna-coe-working-ds-dev.DataCatalog.table_schema`
            SELECT CURRENT_DATE() AS load_date, *
            FROM `{}.{}..INFORMATION_SCHEMA.TABLE_OPTIONS`
            WHERE option_name="description" """.format(project,dataset.dataset_id)

            query_job = client.query(insert)
            query_job.result()

            tablecnt = tablecnt + 1
    else:
        print("{} project does not contain any datasets.".format(project))

    print("Total tables:{}".format(tablecnt))

def backup_table_metadata_curated(**kwargs):
    """
    backs up table schema level meta data
    this should be a project level loop, no need to have 1 for each project
    """
    client = bigquery.Client('amw-dna-coe-curated')
    datasets = list(client.list_datasets('amw-dna-coe-curated'))
    project = 'amw-dna-coe-curated'

    tablecnt = 0

    if datasets:
        print("Datasets in project {}:".format(project))
        for dataset in datasets:
            if dataset.dataset_id[0:8] == "incoming" or dataset.dataset_id[0:9] == "streaming" or dataset.dataset_id[0:7] == "staging" or dataset.dataset_id[0:4] == "temp":
                continue

            print("working on dataset: {}".format(dataset.dataset_id))
            insert = """INSERT INTO `amw-dna-coe-working-ds-dev.DataCatalog.table_schema`
            SELECT CURRENT_DATE() AS load_date, *
            FROM `{}.{}..INFORMATION_SCHEMA.TABLE_OPTIONS`
            WHERE option_name="description" """.format(project,dataset.dataset_id)

            query_job = client.query(insert)  # API request
            query_job.result()  # Waits for statement to finish

            tablecnt = tablecnt + 1
    else:
        print("{} project does not contain any datasets.".format(project))

    print("Total tables:{}".format(tablecnt))

def backup_table_schema_metadata(**kwargs):
    """
    this will back up all meta data saved in BQ for a given project
    project must be called out in the client object.  could loop passing
    the project in.
    """
    client = bigquery.Client('amw-dna-ingestion-prd')
    datasets = list(client.list_datasets('amw-dna-ingestion-prd'))
    project = 'amw-dna-ingestion-prd'

    tablecnt = 0

    if datasets:
        for dataset in datasets:
            if dataset.dataset_id[0:7] == "ana_biw" or dataset.dataset_id[0:8] == "incoming" or dataset.dataset_id[0:9] == "streaming" or dataset.dataset_id[0:7] == "staging" or dataset.dataset_id[0:4] == "temp":
                continue

            tables = client.list_tables(dataset.dataset_id)

            print("Processing tables from dataset {}".format(dataset.dataset_id))
            for table in tables:
                insert = """INSERT INTO `amw-dna-coe-working-ds-dev.DataCatalog.schema_info`
                SELECT CURRENT_DATE() AS load_date, *
                FROM `{}.{}..INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                WHERE table_name='{}' """.format(table.project, table.dataset_id, table.table_id)
                # API request
                query_job = client.query(insert)
                # Waits for statement to finish
                query_job.result()

                tablecnt = tablecnt + 1
    else:
        print("{} project does not contain any datasets.".format(project))

    print("Total tables:{}".format(tablecnt))

def backup_table_schema_metadata_curated(**kwargs):
    """
    this will back up all table descriptions from BQ meta data
    this can be put in a project level loop with the project as
    a parameter
    """
    client = bigquery.Client('amw-dna-coe-curated')
    # Make an API request.
    datasets = list(client.list_datasets('amw-dna-coe-curated'))
    project = 'amw-dna-coe-curated'

    tablecnt = 0

    if datasets:
        for dataset in datasets:
            if dataset.dataset_id[0:8] == "incoming" or dataset.dataset_id[0:9] == "streaming" or dataset.dataset_id[0:7] == "staging" or dataset.dataset_id[0:4] == "temp":
                continue

            tables = client.list_tables(dataset.dataset_id)

            print("Processing tables from dataset {}".format(dataset.dataset_id))
            for table in tables:
                insert = """INSERT INTO `amw-dna-coe-working-ds-dev.DataCatalog.schema_info`
                SELECT CURRENT_DATE() AS load_date, *
                FROM `{}.{}..INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                WHERE table_name='{}' """.format(table.project, table.dataset_id, table.table_id)
                # API request
                query_job = client.query(insert)
                # Waits for statement to finish
                query_job.result()

                tablecnt = tablecnt + 1
    else:
        print("{} project does not contain any datasets.".format(project))

    print("Total tables:{}".format(tablecnt))

# default arguments for the DAG
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
# this comment exists purely to satisfy the needs of sonarqube
# please disregard it.  It is pointless :)

# instantiate a DAG object
with DAG('gcp_schema_sync_v1',
         description='backup and restore bigquery table metadata',
         schedule_interval="30 6 * * *",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         start_date=days_ago(1),
         catchup=False) as dag:

    START = DummyOperator(task_id="start")
    END = DummyOperator(task_id="end")

    backup_table = PythonOperator(
        task_id='backup_table_metadata',
        python_callable=backup_table_metadata,
        provide_context=True
    )
    backup_schema = PythonOperator(
        task_id='backup_table_schema_metadata',
        python_callable=backup_table_schema_metadata,
        provide_context=True
    )
    backup_table_curated = PythonOperator(
        task_id='backup_table_metadata_curated',
        python_callable=backup_table_metadata_curated,
        provide_context=True
    )
    backup_schema_curated = PythonOperator(
        task_id='backup_table_schema_metadata_curated',
        python_callable=backup_table_schema_metadata_curated,
        provide_context=True
    )
    #pylint: disable=pointless-statement
    START >> backup_table >> backup_table_curated >> backup_schema >>  backup_schema_curated >>END
