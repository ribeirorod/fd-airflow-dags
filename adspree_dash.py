from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from airflow.utils.dates import days_ago

from dash.operators import DashAnalyticsToGCSOperator

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
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = 'adspree_dash'

DASH_CONNECTION_ID = 'adspree-dash-connection'

with DAG(
        'adspree-dash',
        default_args=default_args,
        description='Fetching dash-adspree analytics data',
        schedule_interval='10 0 * * *',
        start_date=days_ago(1),
        tags=['dash', 'adspree', 'reporting'],
        max_active_runs=1
) as dag:
    extract = DashAnalyticsToGCSOperator(
        task_id='extract_dash_analytics',
        object_name='adspree/dash/analytics_{{ ds }}.json',
        bucket=GCP_BUCKET,
        dash_connection_id=DASH_CONNECTION_ID,
        gcs_conn_id=GCP_CONNECTION_ID,
        path='/api/dash/v2/analytics/pivot',
        chunk_size=2000,
        dash_params={
            "timezone": "Europe/Berlin",
            "sort": "-date",
            "time_by": "day",
            "group_by[]": ["offer_id", "partner_id", "advertiser_id", "tsp"]
        },
        date_from='{{ macros.ds_format(macros.ds_add(ds, -31), "%Y-%m-%d", "%Y%m%d") }}',
        date_to='{{ ds_nodash }}'
    )

    staging = GCSToBigQueryOperator(
        autodetect=True,
        task_id='staging',
        write_disposition='WRITE_TRUNCATE',
        destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.stg_{TABLE}',
        bucket=GCP_BUCKET,
        source_format='NEWLINE_DELIMITED_JSON',
        source_objects=['{{ ti.xcom_pull(task_ids=\'extract_dash_analytics\', key="return_value") }}'],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID
    )

    upsert_table = BigQueryUpsertTableOperator(
        task_id='upsert_table',
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": TABLE},
            "schema": {
                "fields": [
                    {"name": "date", "type": "DATE"},
                    {"name": "offer_id", "type": "INTEGER"},
                    {"name": "tsp", "type": "INTEGER"},
                    {"name": "offer_name", "type": "STRING"},
                    {"name": "advertiser_id", "type": "INTEGER"},
                    {"name": "advertiser_name", "type": "STRING"},
                    {"name": "partner_id", "type": "INTEGER"},
                    {"name": "partner_name", "type": "STRING"},
                    {"name": "clicks", "type": "INTEGER"},
                    {"name": "conversions", "type": "INTEGER"},
                    {"name": "cost_approved", "type": "FLOAT"},
                    {"name": "cost_pending", "type": "FLOAT"},
                    {"name": "cost_rejected", "type": "FLOAT"},
                    {"name": "revenue_approved", "type": "FLOAT"},
                    {"name": "revenue_pending", "type": "FLOAT"},
                    {"name": "revenue_rejected", "type": "FLOAT"},
                    {"name": "profit", "type": "FLOAT"},
                    {"name": "epc", "type": "FLOAT"},
                    {"name": "fired_conversions", "type": "INTEGER"},
                    {"name": "margin", "type": "FLOAT"},
                    {"name": "total_cost", "type": "FLOAT"},
                    {"name": "total_revenue", "type": "FLOAT"}
                ]
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
                    T.offer_id = S.offer_id 
                    AND T.advertiser_id = S.advertiser_id 
                    AND T.partner_id = S.partner_id 
                    AND T.tsp = S.tsp 
                    AND T.date = S.date
                WHEN MATCHED THEN
                    UPDATE SET 
                        offer_name = S.offer_name, 
                        advertiser_name = S.advertiser_name,
                        partner_name = S.partner_name,
                        clicks = S.click,
                        conversions = S.conversion,
                        cost_approved = S.cost_approved,
                        cost_pending = S.cost_pending,
                        cost_rejected = S.cost_rejected,
                        revenue_approved = S.revenue_approved,
                        revenue_pending = S.revenue_pending,
                        revenue_rejected = S.revenue_rejected,
                        profit = S.profit,
                        epc = S.epc,
                        fired_conversions = S.fired_conversion,
                        margin = S.margin,
                        total_cost = S.total_cost,
                        total_revenue = S.total_rev
                WHEN NOT MATCHED THEN
                    INSERT (
                        cost_approved, cost_pending, cost_rejected, 
                        revenue_approved, revenue_pending, revenue_rejected,
                        profit, epc, fired_conversions, margin, total_cost, total_revenue,
                        conversions, clicks, offer_id, offer_name, tsp,
                        advertiser_id, advertiser_name, partner_id, partner_name, date) 
                    VALUES(
                        S.cost_approved, S.cost_pending, S.cost_rejected, 
                        S.revenue_approved, S.revenue_pending, S.revenue_rejected,
                        S.profit, S.epc, S.fired_conversion, S.margin, S.total_cost, S.total_rev,
                        S.conversion, S.click, S.offer_id, S.offer_name, S.tsp,
                        S.advertiser_id, S.advertiser_name, S.partner_id, S.partner_name, S.date) 
                """,
        write_disposition='WRITE_APPEND'
    )

    extract >> staging >> upsert_table >> load
