"""
Validate various MAGIC PV/BV measures that should always match once transaction processing is complete
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.python import (BranchPythonOperator,
                                               PythonOperator)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import \
    GCSToBigQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule

DAG_CONFIG = Variable.get('MAGIC_VOLUME_VALIDATION_CONFIG', deserialize_json=True)

AFF_SCHED = {
    "0110": { "Group": "EUR-E", "Affiliates": "150" },
    "0140": { "Group": "EUR-W", "Affiliates": "40,60,90,110,160,210,250,470,480,570" },
    "0640": { "Group": "ANA", "Affiliates": "10" },
    "0740": { "Group": "LATAM", "Affiliates": "170" },
    "1610": { "Group": "APAC-JPN", "Affiliates": "70" },
    "1640": { "Group": "APAC-KOR", "Affiliates": "180" },
    "1740": { "Group": "APAC-SE", "Affiliates": "30,50,100,130,200,220,350,500" },
    "1940": { "Group": "Central Asia", "Affiliates": "420" },
    "2040": { "Group": "India", "Affiliates": "430" },
}

CHECK_WWT03900_TRX_MST_SQL = """
SELECT
    CAST(CASE WHEN ROW_COUNT = 0 THEN 1 ELSE 0 END AS INTEGER) CHECK_VAL
FROM
(
    SELECT
        COUNT(*) ROW_COUNT
    FROM
        WWSMGC01.WWT03900_TRX_MST
    WHERE
        AFF_NO IN ({{ get_affiliate_list(ts_nodash[-6:-2]) }})
    AND PROC_STAT_CD IN ('sn', 'mg', 'ca', 'pc', 'pk', 'rd', 'hm')
    AND BNS_PER_NO = {{ params.BNS_PER_NO }}
)
"""

CHECK_WWT03902_TRX_RESPWN_MST_SQL = """
SELECT
    CAST(CASE WHEN ROW_COUNT = 0 THEN 1 ELSE 0 END AS INTEGER) CHECK_VAL
FROM
(
    SELECT
        COUNT(*) ROW_COUNT
    FROM
        WWSMGC01.WWT03902_TRX_RESPWN_MST
    WHERE
        AFF_NO IN ({{ get_affiliate_list(ts_nodash[-6:-2]) }})
    AND PROC_STAT_CD IN ('sn', 'mg', 'ca', 'pc', 'pk', 'rd', 'hm', 'hr', 'pn')
    AND BNS_PER_NO = {{ params.BNS_PER_NO }}
)
"""

VALIDATION_SQL = """
WITH
    SQ AS (
              SELECT
                  DTL.BNS_PER_NO
                , '{{ get_market_group(ts_nodash[-6:-2]) }}' AS MKT_GRP
                , CUST.AFF_NO
                , DTL.BUS_ENTTY_NO
                , DTL.VOL_TYPE_CD
                , SUM(DTL.PV_QTY) AS PV
                , SUM(DTL.BNS_VOL_QTY) AS BV
              FROM
                  WWSMGC01.WWT03476_VOL_PER_DTL DTL
                      JOIN WWSEBS01.WWT01010_BNS_CUST_MST CUST
                      ON DTL.BNS_CUST_ID = CUST.BNS_CUST_ID
              WHERE
                    DTL.BNS_PER_NO = {{ params.BNS_PER_NO }}
                AND CUST.AFF_NO IN ({{ get_affiliate_list(ts_nodash[-6:-2]) }})
                AND DTL.VOL_TYPE_CD IN ('001', '017')
              GROUP BY
                  DTL.BNS_PER_NO
                , CUST.AFF_NO
                , DTL.BUS_ENTTY_NO
                , DTL.VOL_TYPE_CD

              UNION

              SELECT
                  DTL.BNS_PER_NO
                , '{{ get_market_group(ts_nodash[-6:-2]) }}' AS MKT_GRP
                , CUST.AFF_NO
                , DTL.BUS_ENTTY_NO
                , DTL.VOL_TYPE_CD
                , SUM(DTL.PV_QTY) AS PV
                , SUM(DTL.BNS_VOL_QTY) AS BV
              FROM
                  WWSMGC01.WWT03476_VOL_PER_DTL DTL
                , WWSMGC01.WWT12010_AFF_MST AFF_MST
                , WWSEBS01.WWT01010_BNS_CUST_MST CUST
              WHERE
                    AFF_MST.AMWAY_ALIAS_CUST_ID = DTL.BNS_CUST_ID
                AND DTL.BNS_CUST_ID = CUST.BNS_CUST_ID
                AND CUST.AFF_NO IN ({{ get_affiliate_list(ts_nodash[-6:-2]) }})
                AND DTL.BNS_PER_NO = {{ params.BNS_PER_NO }}
                AND DTL.VOL_TYPE_CD = '086'
              GROUP BY
                  DTL.BNS_PER_NO
                , CUST.AFF_NO
                , DTL.BUS_ENTTY_NO
                , DTL.VOL_TYPE_CD

              UNION

              SELECT
                  BNS_PER_NO
                , '{{ get_market_group(ts_nodash[-6:-2]) }}' AS MKT_GRP
                , AFF_NO
                , BUS_ENTTY_NO
                , 'TX001' AS VOL_TYPE_CD
                , SUM(VOL.PV_QTY) AS PV
                , SUM(VOL.BNS_VOL_QTY) AS BV
              FROM
                  WWSMGC01.WWT03900_TRX_MST TRX
                      INNER JOIN WWSMGC01.WWT03710_TRX_VOL_DTL VOL
                      ON TRX.BNS_TRX_ID = VOL.BNS_TRX_ID
              WHERE
                    VOL_TYPE_CD = '001'
                AND TRX.PROC_STAT_CD IN ('cp','pn')
                AND AFF_NO IN ({{ get_affiliate_list(ts_nodash[-6:-2]) }})
                AND BNS_PER_NO = {{ params.BNS_PER_NO }}
              GROUP BY
                  BNS_PER_NO
                , AFF_NO
                , BUS_ENTTY_NO
                , 'TX001'
    )
SELECT
    *
FROM
    SQ PIVOT ( SUM(PV) PV, SUM(BV) BV FOR VOL_TYPE_CD IN ('001' AS V001, '017' V017, '086' AS V086, 'TX001' AS TX001) )
ORDER BY
    1
  , 2
  , 3
"""

SCHEMA=[
      {
        "mode": "NULLABLE",
        "name": "BNS_PER_NO",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "MKT_GRP",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "AFF_NO",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "BUS_ENTTY_NO",
        "type": "INTEGER"
      },
      {
        "mode": "NULLABLE",
        "name": "V001_PV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "V017_PV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "V086_PV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "TX001_PV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "V001_BV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "V017_BV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "V086_BV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "TX001_BV",
        "type": "FLOAT"
      },
      {
        "mode": "NULLABLE",
        "name": "PV_ALL_MATCHING",
        "type": "BOOLEAN"
      },
      {
        "mode": "NULLABLE",
        "name": "BV_ALL_MATCHING",
        "type": "BOOLEAN"
      },
      {
        "mode": "NULLABLE",
        "name": "ALL_MATCHING",
        "type": "STRING"
      },
      {
        "mode": "NULLABLE",
        "name": "SCHEDULED_TS",
        "type": "TIMESTAMP"
      },
      {
        "mode": "NULLABLE",
        "name": "ACTUAL_TS",
        "type": "TIMESTAMP"
      }
    ]

def is_time_to_run(times_to_run, run_now_task_id, skip_run_task_id):
    """See if today is a day to run"""
    context = get_current_context()
    run_time = context["logical_date"].strftime("%H%M")
    print('run_time=' + run_time)
    if run_time in times_to_run:
        return run_now_task_id
    else:
        return skip_run_task_id

def get_bonus_per_no():
    """
    Calculate and return the correct bns_per_no
    """
    from dateutil.relativedelta import relativedelta

    if datetime.now().day <= 7:
        return (datetime.now() + relativedelta(months=-1)).strftime("%Y%m")
    else:
        return datetime.now().strftime("%Y%m")

def extract_data(CONN_ID, SQL, EXTRACT_URI):
    """Extract four kinds of volume for each business entity under the list of affiliates to process

    Args:
        CONN_ID (String): _description_
        SQL (String): _description_
        EXTRACT_URI (String): _description_
    """
    import pytz
    from airflow.providers.oracle.hooks.oracle import OracleHook

    oracle_hook = OracleHook(CONN_ID)
    ctx = get_current_context()
    df = oracle_hook.get_pandas_df(sql=SQL)
    print(SQL)
    df['PV_ALL_MATCHING'] = df.apply(lambda x: x.V001_PV == x.V017_PV == x.V086_PV == x.TX001_PV, axis = 1)
    df['BV_ALL_MATCHING'] = df.apply(lambda x: x.V001_BV == x.V017_BV == x.V086_BV == x.TX001_BV, axis = 1)
    df['ALL_MATCHING'] = df.apply(lambda x: 'MATCHED' if x.PV_ALL_MATCHING == x.BV_ALL_MATCHING == True else 'UNMATCHED', axis=1)
    df['SCHEDULED_TS'] = ctx['logical_date'].isoformat(sep=' ')
    #Reorder columns
    timeZ_Ny = pytz.timezone('America/New_York')
    dt_Ny = datetime.now(timeZ_Ny)
    df['ACTUAL_TS'] = dt_Ny.isoformat(sep=' ')
    cols = ['BNS_PER_NO', 'MKT_GRP', 'AFF_NO', 'BUS_ENTTY_NO', 'V001_PV', 'V017_PV', 'V086_PV', 'TX001_PV', 'V001_BV', 'V017_BV', 'V086_BV', 'TX001_BV', 'PV_ALL_MATCHING', 'BV_ALL_MATCHING', 'ALL_MATCHING', 'SCHEDULED_TS', 'ACTUAL_TS']
    df = df[cols]
    df.to_csv(EXTRACT_URI,index = False)
    status = None
    bus_entty_list = []
    if len(df[df['ALL_MATCHING'] == 'UNMATCHED']) > 0:
        status = 'UNMATCHED'
        bus_entty_list = list(df[df['ALL_MATCHING'] == 'UNMATCHED']['BUS_ENTTY_NO'].values)
        bus_entities = ','.join(str(item) for item in bus_entty_list)
        ctx['ti'].xcom_push(key='bus_entities', value=bus_entities)
    else:
        status = 'MATCHED'
    market_group = get_market_group(ctx['ts_nodash'][-6:-2])
    ctx['ti'].xcom_push(key='status', value=status)
    ctx['ti'].xcom_push(key='market_group', value=market_group)

def get_affiliate_list(run_time):
    """
    Get the list of affiliates to include in the SQL queries
    """
    return AFF_SCHED[run_time]['Affiliates']

def  get_market_group(run_time):
    """
    Get the list of affiliates to include in the SQL queries
    """
    return AFF_SCHED[run_time]['Group']

def send_slack_alert():
    """
    Send an alert to a slack channel on failure
    """
    import pytz
    from airflow.providers.slack.operators.slack_webhook import \
        SlackWebhookOperator

    ctx = get_current_context()
    ti = ctx['ti']
    status = ti.xcom_pull(key="status", task_ids="extract_data")
    timeZ_Ny = pytz.timezone('America/New_York')
    dt_Ny = datetime.now(timeZ_Ny)
    if status == "UNMATCHED":
        market_group = ti.xcom_pull(key="market_group", task_ids="extract_data")
        bus_entities = ti.xcom_pull(key="bus_entities", task_ids="extract_data")
        slack_msg = f"""
        :red_circle: *Validation failed*.

        *Market Group*: {market_group}
        *Countries*: {bus_entities}
        *Validation Time*: {dt_Ny.isoformat(sep=' ',timespec='seconds')}

        *Dashboard*:

        {DAG_CONFIG['TABLEAU_URL']}
        """
        send_alert = SlackWebhookOperator(
            task_id='slack_alert',
            slack_webhook_conn_id=DAG_CONFIG['SLACK_CONN'],
            channel=DAG_CONFIG['SLACK_CHANNEL'],
            username='Google Cloud Composer',
            message=slack_msg
        )
        return send_alert.execute(context=ctx)

def txn_pending_alert(context):
    """
    Send an alert to a slack channel if the sensor(s) times out
    """
    import pytz
    from airflow.providers.slack.operators.slack_webhook import \
        SlackWebhookOperator

    print(f"context={context}")
    scheduled_time = context['ts_nodash'][-6:-2]

    timeZ_Ny = pytz.timezone('America/New_York')
    dt_Ny = datetime.now(timeZ_Ny)
    slack_msg = f"""
    :large_yellow_circle: *Pending transactions found - skipping validation*.

    *Market Group*: {get_market_group(scheduled_time)}
    *Affiliates*: {get_affiliate_list(scheduled_time)}

    *Validation Time*: {dt_Ny.isoformat(sep=' ',timespec='seconds')}

    *Dashboard*:

    {DAG_CONFIG['TABLEAU_URL']}
    """
    send_alert = SlackWebhookOperator(
        task_id='txn_pending_alert',
        slack_webhook_conn_id=DAG_CONFIG['SLACK_CONN'],
        channel=DAG_CONFIG['SLACK_CHANNEL'],
        username='Google Cloud Composer',
        message=slack_msg
    )
    return send_alert.execute(context=context)

def refresh_tableau_ds():
    """
    Refresh Tableau Datasource
    """
    import tableauserverclient as TSC
    from airflow.hooks.base_hook import BaseHook

    tableau_connection = BaseHook.get_connection('tableau_server')
    tableau_auth = TSC.TableauAuth(tableau_connection.login, tableau_connection.password, tableau_connection.schema)
    server = TSC.Server(tableau_connection.host, use_server_version=True, http_options={'verify': False})
    with server.auth.sign_in(tableau_auth):
        ds = server.datasources.get_by_id(DAG_CONFIG['TABLEAU_DATASOURCE_ID'])
        refreshed_ds = server.datasources.refresh(ds)
        print(refreshed_ds)

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['shane.graham@amway.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15),
    'weight_rule': 'upstream'
    # 'on_retry_callback': msteams_task_retry_alert,
    # 'on_failure_callback': msteams_task_fail_alert
}

with DAG('magic_volume_validation_v1',
         description='Validate various MAGIC PV/BV measures that should always match once transaction processing is complete',
         schedule_interval='10,40 * * * *',
         default_args=DEFAULT_ARGS,
         concurrency=12,
         max_active_runs=4,
         start_date=datetime(2022,11,29),
         user_defined_macros={
            'get_affiliate_list': get_affiliate_list,
            'get_market_group': get_market_group
         },
         catchup=True) as dag:

    START = EmptyOperator(task_id="start")


    TIMES_TO_RUN = list(AFF_SCHED.keys())
    EXTRACT_PATH = f"{DAG_CONFIG['GCS_PATH']}/{DAG_CONFIG['EXTRACT_BASE_NAME']}"
    EXTRACT_PATH = EXTRACT_PATH + "{{ ts_nodash }}.csv"
    EXTRACT_URI = f"gs://{DAG_CONFIG['GCS_BUCKET']}/{EXTRACT_PATH}"

    CHECK_TIME_TO_RUN = BranchPythonOperator(
        task_id='check_time_to_run',
        python_callable=is_time_to_run,
        op_args=[
            TIMES_TO_RUN,
            f'do_validation',
            f'skip_validation'
        ],
        dag=dag
    )

    DO_VALIDATION = EmptyOperator(task_id='do_validation')
    SKIP_VALIDATION = EmptyOperator(task_id='skip_validation')

    CHECK_WWT03900_TRX_MST = SqlSensor(
        task_id="check_wwt03900_trx_mst",
        conn_id="oracle_magic_dg",
        sql=CHECK_WWT03900_TRX_MST_SQL,
        mode='reschedule',
        poke_interval=DAG_CONFIG['SENSOR_INTERVAL'],
        timeout=DAG_CONFIG['SENSOR_TIMEOUT'],
        params={
            "BNS_PER_NO": get_bonus_per_no()
        },
        on_failure_callback=txn_pending_alert,
        dag=dag
    )

    CHECK_WWT03902_TRX_RESPWN_MST = SqlSensor(
        task_id="check_wwt03902_trx_respwn_mst",
        conn_id="oracle_magic_dg",
        sql=CHECK_WWT03902_TRX_RESPWN_MST_SQL,
        mode='reschedule',
        poke_interval=DAG_CONFIG['SENSOR_INTERVAL'],
        timeout=DAG_CONFIG['SENSOR_TIMEOUT'],
        params={
            "BNS_PER_NO": get_bonus_per_no()
        },
        on_failure_callback=txn_pending_alert,
        dag=dag
    )

    EXTRACT_DATA = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        op_kwargs={'CONN_ID': 'oracle_magic_dg',
                    'SQL': VALIDATION_SQL,
                    'EXTRACT_URI': EXTRACT_URI
                    },
        params={
            "BNS_PER_NO": get_bonus_per_no(),
            "SCHEDULED_TS": "{{ ts }}"
        },
        do_xcom_push=True,
        dag=dag
    )

    GCS_TO_BQ = GCSToBigQueryOperator(
        task_id="append_data_to_bigquery",
        bucket=DAG_CONFIG['GCS_BUCKET'],
        source_objects=EXTRACT_PATH,
        destination_project_dataset_table=DAG_CONFIG['BIGQUERY_TABLESPEC'],
        autodetect=False,
        skip_leading_rows=1,
        schema_fields=SCHEMA,
        create_disposition='CREATE_IF_NEEDED',
        source_format='CSV',
        write_disposition='WRITE_APPEND',
        dag=dag,
    )

    REFRESH_TABLEAU = PythonOperator(
        task_id="refresh_tableau_datasource",
        python_callable=refresh_tableau_ds,
        dag=dag
    )

    SEND_SLACK_ALERT = PythonOperator(
        task_id="send_slack",
        python_callable=send_slack_alert,
        dag=dag
    )

    END = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    START >> CHECK_TIME_TO_RUN >> [ DO_VALIDATION, SKIP_VALIDATION ]
    DO_VALIDATION >> [ CHECK_WWT03900_TRX_MST, CHECK_WWT03902_TRX_RESPWN_MST ] >> EXTRACT_DATA >> GCS_TO_BQ >> REFRESH_TABLEAU >> SEND_SLACK_ALERT >> END
    SKIP_VALIDATION >> END
