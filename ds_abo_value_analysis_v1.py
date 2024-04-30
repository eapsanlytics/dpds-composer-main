"""
ds_abo_value_analysis.py

"""
from datetime import datetime, timedelta, date
from io import StringIO
import time

import pandas as pd
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
from ingestion.alerts.custom_ms_teams_webhook_operator import CustomMSTeamsWebhookOperator
from ingestion import bq_utility

MSTEAMS_ALERT_CONN_ID = "msteams_dss_airflow_alerts"
MSTEAMS_PIPELINE_CONN_ID = "msteams_dss_pipeline_notices"
#DAG_CONFIG = Variable.get('DS_APR_ORDER_DAG_CONFIG', deserialize_json=True)

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

DATA_PULL_TEMPLATE = """
    CREATE OR REPLACE TABLE
    amw-dna-coe-working-ds-dev.revathi_b_working_ds.ds_det_daily_stage1 AS (
    SELECT
        DISTINCT global_vol_account_id,
        ord_id,
        ord_mo_yr_id,
        ord_dt,
        SUM(adj_ln_usd_net) AS adj_paid,
        SUM(adj_ln_pv) AS adj_pv,
        SUM(adj_ln_bv) AS adj_bv
    FROM
        `amw-dna-coe-curated.demand.ds_det_daily_v`
    WHERE
        ord_canc_flag=FALSE
        AND oper_aff_id=130 --this will probably need to be parameter
        AND ord_mo_yr_id>=200701 -- parameter
        AND global_vol_account_id=global_account_id
    GROUP BY
        1,
        2,
        3,
        4
    HAVING
        adj_pv>0
        AND adj_bv>0);

    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.sales_plan_info_stage1` AS (
    SELECT
        DISTINCT global_account_id,
        year_month,
        account_age_in_months/12 AS tenure_in_years,
        silver_month,
        account_class_code,
        new_percent_level_flag,
        producer_flag,
        performance_bonus_percent,
        personal_pv_normalized_to_10k,
        group_pv_normalized_to_10k,
        total_downline_pv_normalized_to_10k,
        personal_bv,
        group_bv,
        total_downline_bv,
        total_bonus_local_currency,
        CASE
        WHEN diamond_title_holder=1 THEN 'Diamond'
        WHEN emerald_title_holder=1 THEN 'Emerald'
        WHEN platinum_title_holder=1 THEN 'Platinum'
        WHEN gold_title_holder=1 THEN 'Gold'
        WHEN silver_title_holder = 1 THEN 'Silver'
        ELSE
        'Others'
    END
        AS award_class,
        is_at_15_pct_or_above,
        CASE
        WHEN leg7_total_downline_pv > 0 THEN 7
        WHEN leg6_total_downline_pv > 0 THEN 6
        WHEN leg5_total_downline_pv > 0 THEN 5
        WHEN leg4_total_downline_pv > 0 THEN 4
        WHEN leg3_total_downline_pv > 0 THEN 3
        WHEN leg2_total_downline_pv > 0 THEN 2
        WHEN leg1_total_downline_pv > 0 THEN 1
        ELSE
        0
    END
        AS legs_with_volume
    FROM
        `amw-dna-coe-curated.sales_plan.sales_plan_info`
    WHERE
        aff_id=130 # FOR considering Taiwan
        AND year_month >= 201001 --parameters fixed
        AND year_month <= 202307 --parameters just last complete month
        AND (business_nature_code='1'
        OR personal_pv<>0) );

    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.activity_frst_3` AS
    WITH
    time_chk AS (
    SELECT
        DISTINCT global_vol_account_id,
        MIN(DATE(ord_dt)) OVER(PARTITION BY global_vol_account_id) AS frst_shp,
        (MIN(DATE(ord_dt)) OVER(PARTITION BY global_vol_account_id))+90 AS frst_3_month,
        (MIN(DATE(ord_dt)) OVER(PARTITION BY global_vol_account_id))+91 AS after_3_initial,
        (MIN(DATE(ord_dt)) OVER(PARTITION BY global_vol_account_id))+91+365 AS after_3_end,
        adj_pv,
        ord_id,
        ord_dt
    FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.ds_det_daily_stage1` --
    WHERE
        global_vol_account_id IN (
        SELECT
        DISTINCT global_vol_account_id --
        FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.ds_det_daily_stage1`
        GROUP BY
        1 --
        HAVING
        MIN(ord_mo_yr_id) BETWEEN 201501 -- fixed 
        AND 202301 -- 7 months ago from run time
    ) ) ( SELECT
        global_vol_account_id
    FROM
        time_chk
    WHERE
        ord_dt <=frst_3_month
    GROUP BY
        1
    HAVING
        COUNT(DISTINCT ord_id)>=8); 

    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.demand_ds_metrics` AS (
    SELECT
        * EXCEPT(days_since_lst_shp),
        MIN(days_since_lst_shp) AS days_since_last_shop
    FROM (
        SELECT
        DISTINCT global_vol_account_id,
        ord_mo_yr_id,
        SUM(daily_orders) OVER(PARTITION BY global_vol_account_id, ord_mo_yr_id) AS monthly_orders,
        SUM(daily_sales) OVER(PARTITION BY global_vol_account_id, ord_mo_yr_id) AS monthly_sales,
        SUM(demand_pv) OVER(PARTITION BY global_vol_account_id, ord_mo_yr_id) AS demand_pv,
        SUM(demand_bv) OVER(PARTITION BY global_vol_account_id, ord_mo_yr_id) AS demand_bv,
        AVG(CASE
            WHEN shp_rank=1 THEN avg_shp_gap
        END
            ) OVER(PARTITION BY global_vol_account_id, ord_mo_yr_id) AS avg_shop_gap,
        EXTRACT(day
        FROM
            MAX(DATE(ord_dt)) OVER(PARTITION BY ord_mo_yr_id)-MAX(DATE(ord_dt)) OVER(PARTITION BY global_vol_account_id ORDER BY ord_mo_yr_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS days_since_lst_shp
        FROM (
        SELECT
            *,
            AVG(shp_gap) OVER(PARTITION BY global_vol_account_id ORDER BY UNIX_DATE(ord_dt) RANGE BETWEEN 60 PRECEDING
            AND CURRENT ROW) AS avg_shp_gap
        FROM (
            SELECT
            *,
            EXTRACT(day
            FROM
                DATE(ord_dt)-LAG(DATE(ord_dt)) OVER(PARTITION BY global_vol_account_id ORDER BY ord_dt)) AS shp_gap,
            ROW_NUMBER() OVER(PARTITION BY global_vol_account_id, ord_mo_yr_id ORDER BY ord_dt DESC) AS shp_rank,
            MAX(DATE(ord_dt)) OVER(PARTITION BY ord_mo_yr_id) AS month_lst_date,
            MAX(DATE(ord_dt)) OVER(PARTITION BY global_vol_account_id ORDER BY ord_mo_yr_id) AS month_lst_date_acc
            FROM (
            SELECT
                global_vol_account_id,
                ord_mo_yr_id,
                EXTRACT(year
                FROM
                ord_dt)*10000+EXTRACT(month
                FROM
                ord_dt)*100+EXTRACT(day
                FROM
                ord_dt) ord_dt_num,
                DATE(ord_dt) AS ord_dt,
                COUNT(DISTINCT ord_id) AS daily_orders,
                SUM(adj_paid) AS daily_sales,
                SUM(adj_pv) AS demand_pv,
                SUM(adj_bv) AS demand_bv
            FROM
                `amw-dna-coe-working-ds-dev.revathi_b_working_ds.ds_det_daily_stage1` 
            GROUP BY
                1,
                2,
                3,
                4
            HAVING
                demand_pv>1 ) ) ))
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7 );


    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.demand_ds_metrics1` AS (
    SELECT
        * EXCEPT(demand_pv),
        ((demand_pv-min_value)/(max_value-min_value))*10000 AS demand_pv
    FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.demand_ds_metrics`,
        (
        SELECT
        MIN(demand_pv) AS min_value,
        MAX(demand_pv) AS max_value
        FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.demand_ds_metrics`) );

    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.metrics2` AS (
    SELECT
        DISTINCT a.*,
        b.* EXCEPT(global_account_id,
        year_month)
    FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.demand_ds_metrics1` a
    INNER JOIN
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.sales_plan_info_stage1` b
    ON
        a.global_vol_account_id=b.global_account_id
        AND a.ord_mo_yr_id=b.year_month );

    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.model_training_month_abos` AS (
    SELECT
        DISTINCT global_vol_account_id
    FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.metrics2`
    WHERE
        ord_mo_yr_id <= 202207  -- 1 year back only complete months - 13 months -> this is dependent on the horizon - if a horizon was 24 months then this would be -25
    );

    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.model_prediction_month_abos` AS (
    SELECT
        DISTINCT global_vol_account_id
    FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.metrics2`
    WHERE
        ord_mo_yr_id=202307 -- the prediction month (last complete month)
    ); 
        
    CREATE OR REPLACE TABLE
    `amw-dna-coe-working-ds-dev.revathi_b_working_ds.metrics_opt` AS (
    SELECT
        a.* EXCEPT(silver_month,
        account_class_code,
        group_pv_normalized_to_10k,
        total_downline_pv_normalized_to_10k,
        personal_bv,
        group_bv,
        total_downline_bv,
        is_at_15_pct_or_above,
        min_value,
        max_value)
    FROM
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.metrics2` a
    INNER JOIN
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.model_training_month_abos` b
    ON
        a.global_vol_account_id=b.global_vol_account_id
    INNER JOIN
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.model_prediction_month_abos` c
    ON
        b.global_vol_account_id =c.global_vol_account_id
    INNER JOIN
        `amw-dna-coe-working-ds-dev.revathi_b_working_ds.activity_frst_3` d
    ON
        c.global_vol_account_id=d.global_vol_account_id
        AND a.global_vol_account_id<>13000006753901 ); -- this ID was causing issue and excluded
"""

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

with DAG('dpds_abo_value_analysis_data_staging_v1',
         description='Workflow to stage data for the ABO Value Analysis project',
         #schedule_interval="13 8 * * 1",
         schedule_interval="@once",
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=1,
         #start_date=days_ago(1),
         start_date=datetime(2023,8,1),
         catchup=False) as dag:

    START = DummyOperator(task_id="start")
    END = DummyOperator(task_id="end")

    stage_abo_data = BashOperator(
        task_id="stage-data",
        bash_command=DATA_PULL_TEMPLATE,
        params={
            #will be data params 
        }
    )

    START >> stage_abo_data >> END
