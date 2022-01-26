import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

GCS_BUCKET = Variable.get("airflow-bucket")
GCP_CONNECTION_ID = "google_cloud_default"

PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
TABLE = "adspree_portal_list"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}


with DAG(
    "adspree-portals-list",
    default_args=default_args,
    description="Adspree Portal List",
    schedule_interval=None,
    start_date=days_ago(0),
    tags=["adspree", "internal", "manual"],
) as dag:
    filename = "adspree/portals/portals_list_{{ ds }}.csv"
    upload_csv = LocalFilesystemToGCSOperator(
        task_id="upload_csv",
        src=f"{os.path.abspath(os.path.dirname(__file__))}/data/adspree_portals.csv",
        dst=filename,
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONNECTION_ID,
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
                    {"name": "dash_id", "type": "INTEGER"},
                    {"name": "dash_display_name", "type": "STRING"},
                    {"name": "dash_legal_name", "type": "STRING"},
                    {"name": "ga_view", "type": "INTEGER"},
                    {"name": "site_url", "type": "STRING"}
                ]
            },
        },
    )

    load = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id="load",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.{TABLE}",
        bucket=GCS_BUCKET,
        source_format="CSV",
        source_objects=[filename],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    )

    upload_csv >> upsert_table >> load
