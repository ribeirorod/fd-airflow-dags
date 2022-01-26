from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from adspree.taboola_operator import TaboolaCostToGCSOperator, TaboolaRevenueToGCSOperator

CONNECTION_REVENUE = 'adspree-taboola-revenue'
CONNECTION_COST = 'adspree-taboola-cost'


GCS_BUCKET = Variable.get("airflow-bucket")
GCP_CONNECTION_ID = "google_cloud_default"
PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = "adspree_taboola"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    "adspree-taboola",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    tags=["adspree", "external"],
    max_active_runs=1
) as dag:

    filename_cost = "adspree/taboola/taboola_cost_{{ ds }}.csv"
    extract_cost = TaboolaCostToGCSOperator(
        task_id="extract_taboola_cost",
        gcs_conn_id=GCP_CONNECTION_ID,
        bucket=GCS_BUCKET,
        object_name=filename_cost,
        date_from="{{ macros.ds_add(ds, -30) }}",
        date_to="{{ ds }}",
        connection_id=CONNECTION_COST
    )

    filename_revenue = "adspree/taboola/taboola_revenue_{{ ds }}.csv"
    extract_revenue = TaboolaRevenueToGCSOperator(
        task_id="extract_taboola_revenue",
        gcs_conn_id=GCP_CONNECTION_ID,
        bucket=GCS_BUCKET,
        object_name=filename_revenue,
        date_from="{{ macros.ds_add(ds, -30) }}",
        date_to="{{ ds }}",
        connection_id=CONNECTION_REVENUE
    )

    staging_cost = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id="staging_cost",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.stg_{TABLE}_cost",
        bucket=GCS_BUCKET,
        source_format="CSV",
        source_objects=[filename_cost],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    )

    staging_revenue = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id="staging_revenue",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.stg_{TABLE}_revenue",
        bucket=GCS_BUCKET,
        source_format="CSV",
        source_objects=[filename_revenue],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    )

    upsert_revenue_table = BigQueryUpsertTableOperator(
        task_id="upsert_revenue_table",
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": f"{TABLE}_revenue"},
            "schema": {
                "fields": [
                    {"name": "date", "type": "DATE"},
                    {"name": "page_views", "type": "INTEGER"},
                    {"name": "page_views_with_ads", "type": "INTEGER"},
                    {"name": "page_views_with_ads_pct", "type": "FLOAT"},
                    {"name": "clicks", "type": "INTEGER"},
                    {"name": "ctr", "type": "FLOAT"},
                    {"name": "ad_cpc", "type": "FLOAT"},
                    {"name": "ad_rpm", "type": "FLOAT"},
                    {"name": "ad_revenue", "type": "FLOAT"},
                    {"name": "currency", "type": "STRING"},
                    {"name": "account_id", "type": "STRING"},
                ]
            },
        },
    )

    upsert_cost_table = BigQueryUpsertTableOperator(
        task_id="upsert_cost_table",
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": f"{TABLE}_cost"},
            "schema": {
                "fields": [
                    {"name": "date", "type": "DATE"},
                    {"name": "date_end_period", "type": "DATE"},
                    {"name": "clicks", "type": "INTEGER"},
                    {"name": "impressions", "type": "INTEGER"},
                    {"name": "visible_impressions", "type": "INTEGER"},
                    {"name": "spent", "type": "FLOAT"},
                    {"name": "conversions_value", "type": "FLOAT"},
                    {"name": "roas", "type": "FLOAT"},
                    {"name": "ctr", "type": "FLOAT"},
                    {"name": "vctr", "type": "FLOAT"},
                    {"name": "cpm", "type": "FLOAT"},
                    {"name": "vcpm", "type": "FLOAT"},
                    {"name": "cpc", "type": "FLOAT"},
                    {"name": "campaigns_num", "type": "INTEGER"},
                    {"name": "cpa", "type": "FLOAT"},
                    {"name": "cpa_clicks", "type": "FLOAT"},
                    {"name": "cpa_views", "type": "FLOAT"},
                    {"name": "cpa_conversion_rate", "type": "FLOAT"},
                    {"name": "cpa_conversion_rate_clicks", "type": "FLOAT"},
                    {"name": "cpa_conversion_rate_views", "type": "FLOAT"},
                    {"name": "cpa_actions_num", "type": "INTEGER"},
                    {"name": "cpa_actions_num_from_clicks", "type": "INTEGER"},
                    {"name": "cpa_actions_num_from_views", "type": "INTEGER"},
                    {"name": "currency", "type": "STRING"},
                ]
            },
        },
    )
    load_cost = BigQueryExecuteQueryOperator(
        task_id="load_cost",
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE}_cost` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}_cost` S
                ON T.date = EXTRACT(DATE FROM S.date) AND T.date_end_period = EXTRACT(DATE FROM S.date_end_period)
                WHEN NOT MATCHED THEN
                    INSERT (
                        date,
                        date_end_period,
                        clicks,
                        impressions,
                        visible_impressions,
                        spent,
                        conversions_value,
                        roas,
                        ctr,
                        vctr,
                        cpm,
                        vcpm,
                        cpc,
                        campaigns_num,
                        cpa,
                        cpa_clicks,
                        cpa_views,
                        cpa_conversion_rate,
                        cpa_conversion_rate_clicks,
                        cpa_conversion_rate_views,
                        cpa_actions_num,
                        cpa_actions_num_from_clicks,
                        cpa_actions_num_from_views,
                        currency
                    ) 
                    VALUES (
                        EXTRACT(DATE FROM S.date),
                        EXTRACT(DATE FROM S.date_end_period),
                        S.clicks,
                        S.impressions,
                        S.visible_impressions,
                        S.spent,
                        S.conversions_value,
                        S.roas,
                        S.ctr,
                        S.vctr,
                        S.cpm,
                        S.vcpm,
                        S.cpc,
                        S.campaigns_num,
                        S.cpa,
                        S.cpa_clicks,
                        S.cpa_views,
                        S.cpa_conversion_rate,
                        S.cpa_conversion_rate_clicks,
                        S.cpa_conversion_rate_views,
                        S.cpa_actions_num,
                        S.cpa_actions_num_from_clicks,
                        S.cpa_actions_num_from_views,
                        S.currency
                    )
                """,
        write_disposition="WRITE_APPEND",
    )
    load_revenue = BigQueryExecuteQueryOperator(
        task_id="load_revenue",
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE}_revenue` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}_revenue` S
                ON T.date = EXTRACT(DATE FROM S.date) AND T.account_id = S.account_id
                WHEN NOT MATCHED THEN
                    INSERT (
                        date,
                        page_views,
                        page_views_with_ads,
                        page_views_with_ads_pct,
                        clicks,
                        ctr,
                        ad_cpc,
                        ad_rpm,
                        ad_revenue,
                        currency,
                        account_id
                    )
                    VALUES (
                        EXTRACT(DATE FROM S.date),
                        CAST(S.page_views AS INTEGER),
                        CAST(S.page_views_with_ads AS INTEGER),
                        S.page_views_with_ads_pct,
                        CAST(S.clicks AS INTEGER),
                        S.ctr,
                        S.ad_cpc,
                        S.ad_rpm,
                        S.ad_revenue,
                        S.currency,
                        S.account_id
                    )
                """,
        write_disposition="WRITE_APPEND",
    )
    extract_cost >> staging_cost >> upsert_cost_table >> load_cost
    extract_revenue >> staging_revenue >> upsert_revenue_table >> load_revenue
