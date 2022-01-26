from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.utils.dates import days_ago

from dash.operators import DashToGCSOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

GCP_CONNECTION_ID = 'google_cloud_default'
GCP_BUCKET = Variable.get('airflow-bucket')

PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('applift-dataset', default_var="dip_sandbox")

DASH_CONNECTION_ID = 'dash-connection'


class DashTask:
    def __init__(self, name: str, api_path: str, dash_connection_id: str, table_fields: list = None,
                 merge_sql: str = None, dash_params: dict = {}):
        self.name = name
        self.api_path = api_path
        self.dash_connection_id = dash_connection_id
        self.table_fields = table_fields
        self.merge_sql = merge_sql
        self.dash_params = dash_params


with DAG(
        'load-dash-data-v2',
        default_args=default_args,
        description='Dash calls',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(0),
        tags=['dash', 'metadata', 'dimensions'],
) as dag:
    tasks = [
        DashTask('blocked_partners', '/api/internal/v1/blocking/partners', DASH_CONNECTION_ID),
        DashTask('blocked_links', '/api/internal/v1/blocking/tsp', DASH_CONNECTION_ID),
        DashTask('users', '/api/dash/v1/users', DASH_CONNECTION_ID),
        DashTask('advertisers', '/api/dash/v1/advertisers', DASH_CONNECTION_ID),
        DashTask(name='offers',
                 api_path='/api/dash/v1/offers',
                 dash_connection_id=DASH_CONNECTION_ID,
                 dash_params={"sort": "-id"},
                 table_fields=[
                    {"name": "offer_id", "type": "INTEGER"},
                    {"name": "offer_name", "type": "STRING"},
                    {"name": "advertiser_id", "type": "INTEGER"},
                    {"name": "advertiser_name", "type": "STRING"}],
                 merge_sql=f"""
                    MERGE `{PROJECT_NAME}.{DATASET_NAME}.dash_offers` T
                    USING `{PROJECT_NAME}.{DATASET_NAME}.stg_dash_offers` S
                    ON T.offer_id = S.id
                    WHEN MATCHED THEN
                        UPDATE SET 
                            offer_name = S.name, 
                            advertiser_id = S.advertiser_id, 
                            advertiser_name = S.advertiser_short
                    WHEN NOT MATCHED THEN
                        INSERT (offer_id, offer_name, advertiser_id, advertiser_name) 
                        VALUES(S.id, S.name, S.advertiser_id, S.advertiser_short ) 
                    """,
                 ),
        DashTask('partners', '/api/dash/v1/partners', DASH_CONNECTION_ID),
    ]
    for task in tasks:
        extract = DashToGCSOperator(
            task_id=f'extract_{task.name}',
            object_name=f'dash/{task.name}_{{{{ ds }}}}.json',
            bucket=GCP_BUCKET,
            dash_connection_id=task.dash_connection_id,
            gcs_conn_id=GCP_CONNECTION_ID,
            path=task.api_path,
            dash_params=task.dash_params
        )

        staging = GCSToBigQueryOperator(
            autodetect=True,
            task_id=f'staging_{task.name}',
            write_disposition='WRITE_TRUNCATE',
            destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.stg_dash_{task.name}',
            bucket=GCP_BUCKET,
            source_format='NEWLINE_DELIMITED_JSON',
            source_objects=['{{ ti.xcom_pull(task_ids=\'extract_%s\', key="return_value") }}' % (task.name)],
            bigquery_conn_id=GCP_CONNECTION_ID,
            google_cloud_storage_conn_id=GCP_CONNECTION_ID
        )

        extract >> staging

        if task.merge_sql is not None:
            upsert_table = BigQueryUpsertTableOperator(
                task_id=f'table_{task.name}',
                gcp_conn_id=GCP_CONNECTION_ID,
                dataset_id=DATASET_NAME,
                project_id=PROJECT_NAME,
                table_resource={
                    "tableReference": {"tableId": f"dash_{task.name}"},
                    "schema": {
                        "fields": task.table_fields
                    }
                }
            )
            load = BigQueryExecuteQueryOperator(
                task_id=f'load_{task.name}',
                gcp_conn_id=GCP_CONNECTION_ID,
                use_legacy_sql=False,
                sql=task.merge_sql,
                write_disposition='WRITE_APPEND'
            )
            staging >> upsert_table >> load



