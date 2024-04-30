"""
Loads FC Macro data into a consolidated table, hyperion_rev
Macro Forecast runs on the 9th - make sure there is no conflicts with
that pipeline and this refresh
"""
from datetime import datetime, timedelta, date
from io import StringIO
import time

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from googleapiclient import discovery
from ingestion.alerts.custom_ms_teams_webhook_operator import CustomMSTeamsWebhookOperator
from ingestion import bq_utility
from custom.sensors.bigquery import BigQuerySqlSensor

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"
DAG_CONFIG = Variable.get('DS_HYPERION_REFRESH', deserialize_json=True)

INSERT_QUERY = """SELECT CONCAT(m.calen_yr4, LPAD(CAST(m.calen_mo AS STRING),2, '0')) AS MO_YR_KEY_NO,
        m.cntry_cd AS CNTRY_KEY_NO,
        c.corp_nm AS CNTRY_NM,
        ROUND(m.rev_dist_sales_amt * x.exchg_bdgt_pct, 0) AS HYP_REV
      FROM
        `amw-dna-ingestion-prd.forecasting.fft001_macro` m
      JOIN
        `amw-dna-ingestion-prd.forecasting.fft101_corp` c
      ON
        m.cntry_cd = c.cntry_cd
      LEFT JOIN (
        SELECT
          CNTRY_CD,
          EXCHG_BDGT_PCT
        FROM
          `amw-dna-ingestion-prd.forecasting.fft001_macro`
        WHERE
          calen_yr4 = 2023
          AND calen_mo = EXTRACT(MONTH FROM CURRENT_DATE())
        ) x
      ON
        M.cntry_cd = X.cntry_cd
      WHERE 
        --m.calen_yr4 >= 2010 AND
        --m.calen_yr4 <= 2020 AND
        m.cntry_cd NOT IN ('85','99','CS','NU','OT','PR','61','67','71','72','73','75','76','77','78','80','81')
        AND RIGHT(c.corp_cd, 1) != '1'
        AND (m.calen_yr4 * 12 + m.calen_mo) BETWEEN 24001
        AND (CASE
            WHEN EXTRACT(DAY FROM CURRENT_DATE())>8 THEN EXTRACT(YEAR FROM DATE_ADD(CURRENT_DATE(),INTERVAL -1 MONTH)) * 12
          ELSE
          EXTRACT(YEAR
          FROM
            DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH)) * 12
        END
          +
          CASE
            WHEN EXTRACT(DAY FROM CURRENT_DATE())>8 THEN EXTRACT(MONTH FROM DATE_ADD(CURRENT_DATE(), INTERVAL -1 MONTH))
          ELSE
          EXTRACT(MONTH
          FROM
            DATE_ADD(CURRENT_DATE(), INTERVAL -2 MONTH))
        END
          )
        AND m.cntry_cd <> '42'
        """

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
    'email': ['michael.janzen@amway.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'weight_rule': 'upstream',
    'on_retry_callback': msteams_task_retry_alert,
    'on_failure_callback': msteams_task_fail_alert
}

with DAG('ds_update_hyperion_rev_v1',
         description='Workflow to load fc macro values into a consolidated totals table',
         schedule_interval="10 3 7,8,10,11,12,13,14,15,16,17 * *",
         default_args=DEFAULT_ARGS,
         concurrency=1,
         max_active_runs=1,
         start_date=datetime(2022,11,3),
         catchup=False) as dag:

    START = EmptyOperator(task_id="start")
    END = EmptyOperator(task_id="end")

    REFRESH_HYPERION_REVENUE = BigQueryInsertJobOperator(
        task_id="refresh_hyperion_revenue",
        configuration= {
            "query": {
                "query": INSERT_QUERY,
                "destinationTable": {
                    "projectId": DAG_CONFIG["PROJECT"] ,
                    "datasetId": DAG_CONFIG['DATA_SET'],
                    "tableId": DAG_CONFIG['CURATED_TABLE'],
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        },
        dag=dag
    )
    START >> REFRESH_HYPERION_REVENUE >> END
