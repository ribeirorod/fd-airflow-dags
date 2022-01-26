from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from adspree.adsense_operator import AdsenseToGCSOperator

GCS_BUCKET = Variable.get('airflow-bucket')
PROJECT_NAME = Variable.get('data-project-id')
GCP_CONNECTION_ID = 'google_cloud_default'

DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
STG_TABLE = 'stg_adspree_adsense'
LIVE_TABLE = 'adspree_adsense'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

tasks = [
    {
        'connection_id': 'adspree-adsense',
        'table': 'adspree_adsense',
        'account_id': '8061160645273500'
    },
    {
        'connection_id': 'adspree-adsense-nongaming',
        'table': 'adspree_adsense_nongaming',
        'account_id': '5368277619868977'
    }
]

with DAG(
        'adspree-adsense',
        default_args=default_args,
        description='Adspree',
        schedule_interval="0 0 * * *",
        start_date=days_ago(1),
        tags=['adspree', 'external'],
        max_active_runs=1
) as dag:
    for task in tasks:
        filename = f'adspree/adsense/{task["table"]}_{{{{ ds }}}}.csv'
        extract = AdsenseToGCSOperator(
            task_id=f'extract_{task["table"]}',
            account_id=task['account_id'],
            adsense_connection_id='adspree-adsense',
            metrics=[
                "PAGE_VIEWS",
                "AD_REQUESTS",
                "AD_REQUESTS_COVERAGE",
                "CLICKS",
                "ESTIMATED_EARNINGS"
            ],
            dimensions=["DATE", "DOMAIN_NAME"],
            date_range="LAST_30_DAYS",
            gcs_conn_id=GCP_CONNECTION_ID,
            bucket=GCS_BUCKET,
            object_name=filename
        )

        staging = GCSToBigQueryOperator(
            autodetect=True,
            skip_leading_rows=1,
            task_id=f'staging_{task["table"]}',
            write_disposition='WRITE_TRUNCATE',
            destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.stg_{task["table"]}',
            bucket=GCS_BUCKET,
            source_format='CSV',
            source_objects=[filename],
            bigquery_conn_id=GCP_CONNECTION_ID,
            google_cloud_storage_conn_id=GCP_CONNECTION_ID
        )
        upsert_table = BigQueryUpsertTableOperator(
            task_id=f'upsert_{task["table"]}',
            gcp_conn_id=GCP_CONNECTION_ID,
            dataset_id=DATASET_NAME,
            project_id=PROJECT_NAME,
            table_resource={
                "tableReference": {"tableId": task['table']},
                "schema": {
                    "fields": [
                        {"name": "date", "type": "DATE"},
                        {"name": "domain_name", "type": "STRING"},
                        {"name": "page_views", "type": "INTEGER"},
                        {"name": "ad_requests", "type": "INTEGER"},
                        {"name": "ad_requests_coverage", "type": "FLOAT"},
                        {"name": "clicks", "type": "INTEGER"},
                        {"name": "estimated_earnings", "type": "FLOAT"}
                    ]
                }
            }
        )
        load = BigQueryExecuteQueryOperator(
            task_id=f'load_{task["table"]}',
            gcp_conn_id=GCP_CONNECTION_ID,
            use_legacy_sql=False,
            sql=f"""
                    MERGE `{PROJECT_NAME}.{DATASET_NAME}.{task["table"]}` T
                    USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{task["table"]}` S
                    ON T.date = S.date AND T.domain_name = S.domain_name
                    WHEN MATCHED THEN
                        UPDATE SET
                            page_views = S.page_views,
                            ad_requests = S.ad_requests,
                            ad_requests_coverage = S.ad_requests_coverage,
                            clicks = S.clicks,
                            estimated_earnings = S.estimated_earnings
                    WHEN NOT MATCHED THEN
                        INSERT ROW
                    """,
            write_disposition='WRITE_APPEND'
        )
        extract >> staging >> upsert_table >> load
