from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from adspree.zemanta_operator import ZemantaToGCSOperator

CONNECTION_ID = "adspree-zemanta"

GCS_BUCKET = Variable.get("airflow-bucket")
GCP_CONNECTION_ID = "google_cloud_default"
PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = "adspree_zemanta"


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
    "adspree-zemanta",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    tags=["adspree", "external"],
    max_active_runs=1
) as dag:

    filename = "adspree/zemanta/zemanta_{{ ds }}.csv"
    extract = ZemantaToGCSOperator(
        task_id="extract_zemanta",
        gcs_conn_id=GCP_CONNECTION_ID,
        bucket=GCS_BUCKET,
        object_name=filename,
        date_from="{{ macros.ds_add(ds, -30) }}",
        date_to="{{ ds }}",
        connection_id=CONNECTION_ID
    )

    staging = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id="staging",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.stg_{TABLE}",
        bucket=GCS_BUCKET,
        source_format="CSV",
        source_objects=[filename],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    )

    upsert_table = BigQueryUpsertTableOperator(
        task_id="upsert_table",
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": f"{TABLE}"},
            "schema": {
                "fields": [
                    {"name": "day", "type": "DATE"},
                    {"name": "account_id", "type": "INTEGER"},
                    {"name": "account", "type": "STRING"},
                    {"name": "campaign_id", "type": "INTEGER"},
                    {"name": "campaign", "type": "STRING"},
                    {"name": "ad_group_id", "type": "INTEGER"},
                    {"name": "ad_group", "type": "STRING"},
                    {"name": "content_ad_id", "type": "INTEGER"},
                    {"name": "content_ad", "type": "STRING"},
                    {"name": "impressions", "type": "INTEGER"},
                    {"name": "clicks", "type": "INTEGER"},
                    {"name": "ctr", "type": "FLOAT"},
                    {"name": "avg_cpc", "type": "FLOAT"},
                    {"name": "avg_cpm", "type": "FLOAT"},
                    {"name": "media_spend", "type": "FLOAT"},
                    {"name": "license_fee", "type": "FLOAT"},
                    {"name": "total_spend", "type": "FLOAT"},
                    {"name": "margin", "type": "FLOAT"},
                    {"name": "visits", "type": "INTEGER"},
                    {"name": "unique_users", "type": "INTEGER"},
                    {"name": "new_users", "type": "INTEGER"},
                    {"name": "returning_users", "type": "INTEGER"},
                    {"name": "new_users_pct", "type": "FLOAT"},
                    {"name": "pageviews", "type": "INTEGER"},
                    {"name": "pageviews_per_visit", "type": "INTEGER"},
                    {"name": "bounced_visits", "type": "INTEGER"},
                    {"name": "non_bounced_visits", "type": "INTEGER"},
                    {"name": "bounce_rate", "type": "FLOAT"},
                ]
            },
        },
    )
    load = BigQueryExecuteQueryOperator(
        task_id="load",
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}` S
                ON T.day = S.day 
                AND T.account_id = S.account_id 
                AND T.campaign_id = S.campaign_id
                AND T.ad_group_id = S.ad_group_id
                AND T.content_ad_id = S.content_ad_id
                WHEN MATCHED THEN
                    UPDATE SET
                        account = S.account,
                        campaign = S.campaign,
                        content_ad = S.content_ad,
                        impressions = S.impressions,
                        clicks = S.clicks,
                        ctr = S.ctr,
                        avg_cpc = S.avg_cpc,
                        avg_cpm = S.avg_cpm,
                        media_spend = S.media_spend,
                        license_fee = S.license_fee,
                        total_spend = S.total_spend,
                        margin = S.margin,
                        visits = CAST(S.visits AS INTEGER),
                        unique_users = CAST(S.unique_users AS INTEGER),
                        new_users = CAST(S.new_users AS INTEGER),
                        returning_users = CAST(S.returning_users AS INTEGER),
                        new_users_pct = S.new_users_pct,
                        pageviews = CAST(S.pageviews AS INTEGER),
                        pageviews_per_visit = CAST(S.pageviews_per_visit AS INTEGER),
                        bounced_visits = CAST(S.bounced_visits AS INTEGER),
                        non_bounced_visits = CAST(S.non_bounced_visits AS INTEGER),
                        bounce_rate = S.bounce_rate
                WHEN NOT MATCHED THEN
                    INSERT (
                        day,
                        account_id,
                        account,
                        campaign_id,
                        campaign,
                        ad_group_id,
                        ad_group,
                        content_ad_id,
                        content_ad,
                        impressions,
                        clicks,
                        ctr,
                        avg_cpc,
                        avg_cpm,
                        media_spend,
                        license_fee,
                        total_spend,
                        margin,
                        visits,
                        unique_users,
                        new_users,
                        returning_users,
                        new_users_pct,
                        pageviews,
                        pageviews_per_visit,
                        bounced_visits,
                        non_bounced_visits,
                        bounce_rate
                    )
                    VALUES (
                        S.day,
                        S.account_id,
                        S.account,
                        S.campaign_id,
                        S.campaign,
                        S.ad_group_id,
                        S.ad_group,
                        S.content_ad_id,
                        S.content_ad,
                        S.impressions,
                        S.clicks,
                        S.ctr,
                        S.avg_cpc,
                        S.avg_cpm,
                        S.media_spend,
                        S.license_fee,
                        S.total_spend,
                        S.margin,
                        CAST(S.visits AS INTEGER),
                        CAST(S.unique_users AS INTEGER),
                        CAST(S.new_users AS INTEGER),
                        CAST(S.returning_users AS INTEGER),
                        S.new_users_pct,
                        CAST(S.pageviews AS INTEGER),
                        CAST(S.pageviews_per_visit AS INTEGER),
                        CAST(S.bounced_visits AS INTEGER),
                        CAST(S.non_bounced_visits AS INTEGER),
                        S.bounce_rate
                    )
                """,
        write_disposition="WRITE_APPEND",
    )
    extract >> staging >> upsert_table >> load
