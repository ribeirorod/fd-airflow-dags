from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from adspree.revcontent_operator import RevcontentToGCSOperator

REVCONTENT_CONNECTION_ID = "adspree-revcontent"

GCS_BUCKET = Variable.get("airflow-bucket")
GCP_CONNECTION_ID = "google_cloud_default"
PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
STG_TABLE = "stg_adspree_revcontent"
LIVE_TABLE = "adspree_revcontent"


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
    "adspree-revcontent",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(0),
    tags=['adspree', 'external'],
    max_active_runs=1
) as dag:

    filename = "adspree/revcontent/revcontent_{{ ds }}.csv"
    extract = RevcontentToGCSOperator(
        task_id="extract_revcontent",
        gcs_conn_id=GCP_CONNECTION_ID,
        bucket=GCS_BUCKET,
        object_name=filename,
        date_from="{{ macros.ds_add(ds, -30) }}",
        date_to="{{ ds }}",
        connection_id=REVCONTENT_CONNECTION_ID
    )

    staging = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id="staging",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.{STG_TABLE}",
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
            "tableReference": {"tableId": f"{LIVE_TABLE}"},
            "schema": {
                "fields": [
                    {"name": "bid_type", "type": "STRING"},
                    {"name": "id", "type": "INTEGER"},
                    {"name": "date", "type": "DATE"},
                    {"name": "impressions", "type": "INTEGER"},
                    {"name": "viewable_impressions", "type": "INTEGER"},
                    {"name": "clicks", "type": "INTEGER"},
                    {"name": "conversions", "type": "INTEGER"},
                    {"name": "ctr", "type": "FLOAT"},
                    {"name": "vctr", "type": "FLOAT"},
                    {"name": "avg_cpc", "type": "FLOAT"},
                    {"name": "avg_cpv", "type": "FLOAT"},
                    {"name": "cost", "type": "FLOAT"},
                    {"name": "return", "type": "FLOAT"},
                    {"name": "profit", "type": "FLOAT"},
                    {"name": "target_url", "type": "STRING"},
                ]
            },
        },
    )
    load = BigQueryExecuteQueryOperator(
        task_id="load",
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{LIVE_TABLE}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.{STG_TABLE}` S
                ON 
                    T.id = S.id 
                    AND T.date = S.date 
                    AND T.target_url = S.target_url 
                    AND T.bid_type = S.bid_type
                WHEN NOT MATCHED THEN
                    INSERT (
                        bid_type,
                        id,
                        date,
                        impressions,
                        viewable_impressions,
                        clicks,
                        conversions,
                        ctr,
                        vctr,
                        avg_cpc,
                        avg_cpv,
                        cost,
                        return,
                        profit,
                        target_url
                    )
                    VALUES (
                        S.bid_type,
                        S.id,
                        S.date,
                        S.impressions,
                        S.viewable_impressions,
                        S.clicks,
                        S.conversions,
                        S.ctr,
                        S.vctr,
                        S.avg_cpc,
                        S.avg_vcpm,
                        S.cost,
                        S.return,
                        S.profit,
                        S.target_url
                    )
                    
                WHEN MATCHED THEN
                    UPDATE SET
                        impressions = S.impressions,
                        viewable_impressions = S.impressions,
                        clicks = S.clicks,
                        conversions = S.conversions,
                        ctr = S.ctr,
                        vctr = S.vctr,
                        avg_cpc = S.avg_cpc,
                        avg_cpv = S.avg_vcpm,
                        cost = S.cost,
                        return = S.return,
                        profit = S.profit
                """,
        write_disposition="WRITE_APPEND",
    )
    extract >> staging >> upsert_table >> load
