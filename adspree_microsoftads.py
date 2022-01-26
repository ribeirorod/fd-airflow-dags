from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from adspree.microsoft_ads_operator import MicrosoftAdsToGCSOperator

GCS_BUCKET = Variable.get('airflow-bucket')
GCP_CONNECTION_ID = 'google_cloud_default'

PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = 'adspree_microsoftads'

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
    {'name': 'account_name', 'type': 'STRING'},
    {'name': 'account_number', 'type': 'STRING'},
    {'name': 'account_id', 'type': 'INTEGER'},
    {'name': 'time_period', 'type': 'DATE'},
    {'name': 'campaign_name', 'type': 'STRING'},
    {'name': 'campaign_id', 'type': 'INTEGER'},
    {'name': 'ad_group_name', 'type': 'STRING'},
    {'name': 'ad_group_id', 'type': 'INTEGER'},
    {'name': 'ad_id', 'type': 'INTEGER'},
    {'name': 'currency_code', 'type': 'STRING'},
    {'name': 'ad_distribution', 'type': 'STRING'},
    {'name': 'destination_url', 'type': 'STRING'},
    {'name': 'impressions', 'type': 'INTEGER'},
    {'name': 'clicks', 'type': 'INTEGER'},
    {'name': 'ctr', 'type': 'FLOAT'},
    {'name': 'average_cpc', 'type': 'FLOAT'},
    {'name': 'spend', 'type': 'FLOAT'},
    {'name': 'average_position', 'type': 'STRING'},
    {'name': 'conversions', 'type': 'INTEGER'},
    {'name': 'conversion_rate', 'type': 'FLOAT'},
    {'name': 'cost_per_conversion', 'type': 'FLOAT'},
    {'name': 'device_type', 'type': 'STRING'},
    {'name': 'language', 'type': 'STRING'},
    {'name': 'bid_match_type', 'type': 'STRING'},
    {'name': 'delivered_match_type', 'type': 'STRING'},
    {'name': 'network', 'type': 'STRING'},
    {'name': 'top_vs_other', 'type': 'STRING'},
    {'name': 'device_o_s', 'type': 'STRING'},
    {'name': 'assists', 'type': 'INTEGER'},
    {'name': 'revenue', 'type': 'FLOAT'},
    {'name': 'return_on_ad_spend', 'type': 'STRING'},
    {'name': 'cost_per_assist', 'type': 'FLOAT'},
    {'name': 'revenue_per_conversion', 'type': 'FLOAT'},
    {'name': 'revenue_per_assist', 'type': 'FLOAT'},
    {'name': 'tracking_template', 'type': 'STRING'},
    {'name': 'custom_parameters', 'type': 'STRING'},
    {'name': 'final_url', 'type': 'STRING'},
    {'name': 'final_mobile_url', 'type': 'STRING'},
    {'name': 'final_app_url', 'type': 'STRING'},
    {'name': 'account_status', 'type': 'STRING'},
    {'name': 'campaign_status', 'type': 'STRING'},
    {'name': 'ad_group_status', 'type': 'STRING'},
    {'name': 'ad_status', 'type': 'STRING'},
    {'name': 'customer_id', 'type': 'INTEGER'},
    {'name': 'customer_name', 'type': 'STRING'},
    {'name': 'final_url_suffix', 'type': 'STRING'},
    {'name': 'all_conversions', 'type': 'INTEGER'},
    {'name': 'all_revenue', 'type': 'FLOAT'},
    {'name': 'all_conversion_rate', 'type': 'FLOAT'},
    {'name': 'all_cost_per_conversion', 'type': 'FLOAT'},
    {'name': 'all_return_on_ad_spend', 'type': 'FLOAT'},
    {'name': 'all_revenue_per_conversion', 'type': 'FLOAT'},
    {'name': 'view_through_conversions', 'type': 'INTEGER'},
    {'name': 'goal', 'type': 'STRING'},
    {'name': 'goal_type', 'type': 'STRING'},
    {'name': 'absolute_top_impression_rate_percent', 'type': 'FLOAT'},
    {'name': 'top_impression_rate_percent', 'type': 'FLOAT'},
    {'name': 'average_cpm', 'type': 'FLOAT'},
]

with DAG(
        'adspree-microsoft-ads',
        default_args=default_args,
        description='Adspree',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(0),
        tags=['adspree', 'external'],
        max_active_runs=1
) as dag:
    filename = 'adspree/microsoftads/ads_{{ ds }}.csv'
    extract = MicrosoftAdsToGCSOperator(
        task_id='extract_microsoft_ads',
        bucket=GCS_BUCKET,
        object_name=filename,
        gcs_conn_id=GCP_CONNECTION_ID,
        msads_connection_id='adspree-microsoftads',
        date_from="{{ macros.ds_add(ds, -30) }}",
        date_to="{{ ds }}",
    )

    staging = GCSToBigQueryOperator(
        autodetect=False,
        schema_fields=SCHEMA_FIELDS,
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
    load = BigQueryExecuteQueryOperator(
        task_id='load',
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}` S
                ON T.account_id = S.account_id AND T.time_period = S.time_period
                WHEN NOT MATCHED THEN
                    INSERT ROW
                """, # TODO implement update
        write_disposition='WRITE_APPEND'
    )
    extract >> staging >> upsert_table >> load
