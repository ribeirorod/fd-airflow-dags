from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from adspree.operators import PatchedGoogleAdsToGcsOperator

GCS_BUCKET = Variable.get('airflow-bucket')
GCP_CONNECTION_ID = 'google_cloud_default'

PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = 'adspree_googleads'

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
    {"name": "account_id", "type": "INTEGER"},
    {"name": "account_name", "type": "STRING"},
    {"name": "campaign_id", "type": "INTEGER"},
    {"name": "campaign_name", "type": "STRING"},
    {"name": "adgroup_id", "type": "INTEGER"},
    {"name": "adgroup_name", "type": "STRING"},
    {"name": "final_url", "type": "STRING"},
    {"name": "impressions", "type": "INTEGER"},
    {"name": "clicks", "type": "INTEGER"},
    {"name": "cost_micros", "type": "INTEGER"}
]
with DAG(
        'adspree-googleads',
        default_args=default_args,
        description='Adspree',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(1),
        tags=['adspree', 'external'],
        max_active_runs=1
) as dag:
    filename = 'adspree/googleads/ads_{{ ds }}.csv'
    extract = PatchedGoogleAdsToGcsOperator(
        task_id='extract_google_ads',
        google_ads_conn_id='google-ads-connection',
        bucket=GCS_BUCKET,
        obj=filename,
        attributes=[
            "segments.date",
            "customer.id",
            "customer.descriptive_name",
            "campaign.id",
            "campaign.name",
            "ad_group.id",
            "ad_group.name",
            "expanded_landing_page_view.expanded_final_url",
            "metrics.impressions",
            "metrics.clicks",
            "metrics.cost_micros",
        ],
        client_ids=["7482207429", "7567336610", "5639490749", "5958815276", "5387778919"],
        query=""" 
            SELECT
              segments.date,
              customer.id,
              customer.descriptive_name,
              campaign.id,
              campaign.name,
              ad_group.id,
              ad_group.name,
              expanded_landing_page_view.expanded_final_url,
              metrics.impressions,
              metrics.clicks,
              metrics.cost_micros
            FROM expanded_landing_page_view WHERE segments.date BETWEEN '{{ macros.ds_add(ds, -30) }}' AND '{{ ds }}' 
            """
    )

    staging = GCSToBigQueryOperator(
        autodetect=False,
        schema_fields=SCHEMA_FIELDS,
        skip_leading_rows=0,
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
                ON 
                    T.date = S.date 
                    AND T.account_id = S.account_id 
                    AND T.campaign_id = S.campaign_id
                    AND T.adgroup_id = S.adgroup_id 
                    AND T.final_url = S.final_url
                WHEN NOT MATCHED THEN
                    INSERT (date, account_id, account_name, campaign_id, campaign_name, adgroup_id, adgroup_name, final_url, impressions, clicks, cost_micros)
                    VALUES (S.date, S.account_id, S.account_name, S.campaign_id, S.campaign_name, S.adgroup_id, S.adgroup_name, S.final_url, S.impressions, S.clicks, S.cost_micros)
                WHEN MATCHED THEN
                    UPDATE SET 
                        campaign_name = S.campaign_name,
                        adgroup_name = S.adgroup_name,
                        account_name = S.account_name,
                        impressions = S.impressions, 
                        clicks = S.clicks, 
                        cost_micros = S.cost_micros
                """,
        write_disposition='WRITE_APPEND'
    )
    extract >> staging >> upsert_table >> load
