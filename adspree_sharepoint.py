from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from adspree.sharepoint_operator import SharepointCostToGCSOperator, SharepointRevenueToGCSOperator

CONNECTION_ID = "mediaelements-sharepoint"

GCS_BUCKET = Variable.get("airflow-bucket")
GCP_CONNECTION_ID = "google_cloud_default"
PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = "adspree_sharepoint"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

FIELDS_COST = [
    {"name": "id", "type": "INTEGER"},
    {"name": "year", "type": "INTEGER"},
    {"name": "month", "type": "INTEGER"},
    {"name": "booking_period", "type": "STRING"},
    {"name": "cost_center_id", "type": "INTEGER"},
    {"name": "cost_center_name", "type": "STRING"},
    {"name": "account_manager_id", "type": "INTEGER"},
    {"name": "account_manager_name", "type": "STRING"},
    {"name": "advertiser_id", "type": "INTEGER"},
    {"name": "advertiser_name", "type": "STRING"},
    {"name": "publisher_id", "type": "INTEGER"},
    {"name": "publisher_name", "type": "STRING"},
    {"name": "product_id", "type": "INTEGER"},
    {"name": "product_name", "type": "STRING"},
    {"name": "status", "type": "STRING"},
    {"name": "cost", "type": "FLOAT"},
    {"name": "cost_ist", "type": "FLOAT"},
    {"name": "booking_period_date", "type": "TIMESTAMP"},
    {"name": "comment", "type": "STRING"},
    {"name": "author_id", "type": "INTEGER"},
    {"name": "author_name", "type": "STRING"},
]

FIELDS_REVENUE = [
    {"name": "id", "type": "INTEGER"},
    {"name": "year", "type": "INTEGER"},
    {"name": "month", "type": "INTEGER"},
    {"name": "booking_period", "type": "STRING"},
    {"name": "cost_center_id", "type": "INTEGER"},
    {"name": "cost_center_name", "type": "STRING"},
    {"name": "account_manager_id", "type": "INTEGER"},
    {"name": "account_manager_name", "type": "STRING"},
    {"name": "advertiser_id", "type": "INTEGER"},
    {"name": "advertiser_name", "type": "STRING"},
    {"name": "product_id", "type": "INTEGER"},
    {"name": "product_name", "type": "STRING"},
    {"name": "status", "type": "STRING"},
    {"name": "revenue", "type": "FLOAT"},
    {"name": "revenue_ist", "type": "FLOAT"},
    {"name": "booking_period_date", "type": "TIMESTAMP"},
    {"name": "comment", "type": "STRING"},
    {"name": "author_id", "type": "INTEGER"},
    {"name": "author_name", "type": "STRING"},
]

with DAG(
        "adspree-sharepoint",
        default_args=default_args,
        schedule_interval='0 * * * *',
        start_date=days_ago(1),
        tags=["adspree", "external", "sharepoint"],
        max_active_runs=1
) as dag:
    filename_cost = "adspree/sharepoint/cost_{{ ts_nodash }}.csv"
    filename_revenue = "adspree/sharepoint/revenue_{{ ts_nodash }}.csv"
    extract_cost = SharepointCostToGCSOperator(
        task_id="extract_cost",
        gcs_conn_id=GCP_CONNECTION_ID,
        bucket=GCS_BUCKET,
        object_name=filename_cost,
        connection_id=CONNECTION_ID
    )

    extract_revenue = SharepointRevenueToGCSOperator(
        task_id="extract_revenue",
        gcs_conn_id=GCP_CONNECTION_ID,
        bucket=GCS_BUCKET,
        object_name=filename_revenue,
        connection_id=CONNECTION_ID
    )

    load_cost = GCSToBigQueryOperator(
        autodetect=False,
        schema_fields=FIELDS_COST,
        skip_leading_rows=1,
        task_id="load_cost",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.{TABLE}_cost",
        bucket=GCS_BUCKET,
        source_format="CSV",
        source_objects=[filename_cost],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    )

    load_revenue = GCSToBigQueryOperator(
        autodetect=False,
        schema_fields=FIELDS_REVENUE,
        skip_leading_rows=1,
        task_id="load_revenue",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.{TABLE}_revenue",
        bucket=GCS_BUCKET,
        source_format="CSV",
        source_objects=[filename_revenue],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    )

    extract_cost >> load_cost
    extract_revenue >> load_revenue
