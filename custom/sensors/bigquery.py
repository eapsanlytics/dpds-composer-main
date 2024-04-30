from typing import Any, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud.exceptions import NotFound


class BigQuerySqlSensor(BaseSensorOperator):
    """
    Sensor that waits for a BigQuery SQL query to return results.

    :param sql: The SQL query to execute in BigQuery.
    :type sql: str
    :param bigquery_conn_id: The BigQuery connection ID to use to connect to the
                             BigQuery service. Defaults to 'google_cloud_default'.
    :type bigquery_conn_id: str
    :param project_id: The project ID of the BigQuery project.
    :type project_id: str
    :param poke_interval: Time interval between pokes.
    :type poke_interval: int
    :param timeout: The amount of time (in seconds) to wait between retries
                    for the SQL query to return results. Defaults to 60.
    :type timeout: int
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    ui_color = "#7c7287"

    def __init__(self, sql, gcp_conn_id='google_cloud_default', project_id=None,
                 poke_interval=60, timeout=60, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.poke_interval = poke_interval
        self.timeout = timeout

    def poke(self, context):
        self.log.info('Poking for result from BigQuery SQL: %s', self.sql)
        hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
        try:
            df = hook.get_pandas_df(self.sql, parameters=None, dialect='standard')
            if not df.empty:
                return True
            return False
        except NotFound:
            self.log.info('BigQuery job not found.')
            return False
        except Exception as e:
            self.log.error('Error checking BigQuery job status: %s', str(e))
            raise