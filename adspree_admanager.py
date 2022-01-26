from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from adspree.googleadmanager_operator import GoogleAdManagerToGCSOperator

GCS_BUCKET = Variable.get('airflow-bucket')
GCP_CONNECTION_ID = 'google_cloud_default'

PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = 'adspree_googleadmanager'
TABLE_ADX = 'adspree_googleadmanager_adx'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
SCHEMA_FIELDS = [
    {"name": "date", "type": "DATE"},
    {"name": "domain", "type": "STRING"},
    {"name": "total_line_item_level_impressions", "type": "INTEGER"},
    {"name": "total_line_item_level_clicks", "type": "INTEGER"},
    {"name": "total_line_item_level_cpm_and_cpc_revenue", "type": "INTEGER"},
    {"name": "ad_server_impressions", "type": "INTEGER"},
    {"name": "ad_server_clicks", "type": "INTEGER"},
    {"name": "ad_server_cpm_and_cpc_revenue", "type": "INTEGER"},
    {"name": "ad_server_line_item_level_percent_impressions", "type": "FLOAT"},
    {"name": "adsense_line_item_level_impressions", "type": "INTEGER"},
    {"name": "adsense_line_item_level_clicks", "type": "INTEGER"},
    {"name": "adsense_line_item_level_revenue", "type": "INTEGER"},
    {"name": "adsense_line_item_level_average_ecpm", "type": "INTEGER"},
]

SCHEMA_FIELDS_ADX = [
    {"name": "ad_exchange_site_name", "type": "STRING"},
    {"name": "ad_exchange_date", "type": "DATE"},
    {"name": "ad_exchange_impressions", "type": "INTEGER"},
    {"name": "ad_exchange_clicks", "type": "INTEGER"},
    {"name": "ad_exchange_estimated_revenue", "type": "INTEGER"},
    {"name": "ad_exchange_ad_ecpm", "type": "INTEGER"},
]
with DAG(
        'adspree-googleadmanager',
        default_args=default_args,
        description='Adspree',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(1),
        tags=['adspree', 'external'],
        max_active_runs=1
) as dag:
    filename = 'adspree/googleadmanager/report_{{ ds }}.csv'
    extract = GoogleAdManagerToGCSOperator(
        task_id='extract',
        connection_id='adspree-google-analytics',
        bucket=GCS_BUCKET,
        object_name=filename,
        gcs_conn_id=GCP_CONNECTION_ID,
        date_from='{{ macros.ds_add(ds, -30) }}',
        date_to='{{ ds }}',
        columns=[
            'TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS',
            'TOTAL_LINE_ITEM_LEVEL_CLICKS',
            'TOTAL_LINE_ITEM_LEVEL_CPM_AND_CPC_REVENUE',
            'AD_SERVER_IMPRESSIONS',
            'AD_SERVER_CLICKS',
            'AD_SERVER_CPM_AND_CPC_REVENUE',
            'AD_SERVER_LINE_ITEM_LEVEL_PERCENT_IMPRESSIONS',
            'ADSENSE_LINE_ITEM_LEVEL_IMPRESSIONS',
            'ADSENSE_LINE_ITEM_LEVEL_CLICKS',
            'ADSENSE_LINE_ITEM_LEVEL_REVENUE',
            'ADSENSE_LINE_ITEM_LEVEL_AVERAGE_ECPM',
        ],
        dimensions=[
            'DATE',
            'DOMAIN',
        ],
        network_code=21853020818
    )

    filename_adx = 'adspree/googleadmanager/report_adx_{{ ds }}.csv'
    extract_adx = GoogleAdManagerToGCSOperator(
        task_id='extract_adx',
        connection_id='adspree-google-analytics',
        bucket=GCS_BUCKET,
        object_name=filename_adx,
        gcs_conn_id=GCP_CONNECTION_ID,
        date_from='{{ macros.ds_add(ds, -30) }}',
        date_to='{{ ds }}',
        columns=[
            'AD_EXCHANGE_IMPRESSIONS',
            'AD_EXCHANGE_CLICKS',
            'AD_EXCHANGE_ESTIMATED_REVENUE',
            'AD_EXCHANGE_AD_ECPM',
        ],
        dimensions=[
            'AD_EXCHANGE_SITE_NAME',
            'AD_EXCHANGE_DATE'
        ],
        timezone_type='AD_EXCHANGE',
        network_code=21853020818
    )

    staging = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id='staging',
        write_disposition='WRITE_TRUNCATE',
        destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.stg_{TABLE}',
        bucket=GCS_BUCKET,
        source_format='CSV',
        source_objects=[filename],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID
    )
    staging_adx = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id='staging_adx',
        write_disposition='WRITE_TRUNCATE',
        destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.stg_{TABLE_ADX}',
        bucket=GCS_BUCKET,
        source_format='CSV',
        source_objects=[filename_adx],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID
    )
    upsert_table = BigQueryUpsertTableOperator(
        task_id='upsert_table',
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": f"{TABLE}"},
            "schema": {
                "fields": SCHEMA_FIELDS
            }
        }
    )

    upsert_adx_table = BigQueryUpsertTableOperator(
        task_id='upsert_adx_table',
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": f"{TABLE_ADX}"},
            "schema": {
                "fields": SCHEMA_FIELDS_ADX
            }
        }
    )
    load = BigQueryExecuteQueryOperator(
        task_id='load',
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}` S
                ON 
                    T.date = S.date 
                    AND T.domain = S.domain 
                WHEN NOT MATCHED THEN
                    INSERT ({','.join([x['name'] for x in SCHEMA_FIELDS])})
                    VALUES ({','.join(['S.' + x['name'] for x in SCHEMA_FIELDS])})
                """,
        write_disposition='WRITE_APPEND'
    )

    load_adx = BigQueryExecuteQueryOperator(
        task_id='load_adx',
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE_ADX}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE_ADX}` S
                ON 
                    T.ad_exchange_date = S.ad_exchange_date 
                    AND T.ad_exchange_site_name = S.ad_exchange_site_name 
                WHEN NOT MATCHED THEN
                    INSERT ({','.join([x['name'] for x in SCHEMA_FIELDS_ADX])})
                    VALUES ({','.join(['S.' + x['name'] for x in SCHEMA_FIELDS_ADX])})
                """,
        write_disposition='WRITE_APPEND'
    )
    extract >> staging >> upsert_table >> load
    extract_adx >> staging_adx >> upsert_adx_table >> load_adx
