import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator
from airflow.utils.dates import days_ago

AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")

GCS_BUCKET = Variable.get("airflow-bucket")
GCP_CONNECTION_ID = "google_cloud_default"

PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('applift-dataset', default_var="dip_sandbox")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0
}

with DAG(
        "applift-schemas",
        default_args=default_args,
        description="Create/Update applift schemas",
        schedule_interval=None,
        start_date=days_ago(0),
        tags=["applift", "internal", "manual"],
) as dag:
    BigQueryUpsertTableOperator(
        task_id="upsert_overwatch_clicks",
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": "applift_overwatch_clicks"},
            "timePartitioning": {
                "type": "DAY",
                "field": "ts"
            },
            "schema": {
                "fields": [
                    {"name": "id", "mode": "REQUIRED", "type": "STRING"},
                    {"name": "ts", "mode": "REQUIRED", "type": "TIMESTAMP"},
                    {"name": "sent_at", "mode": "REQUIRED", "type": "TIMESTAMP"},
                    {"mode": "NULLABLE", "name": "isp", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "service_id", "type": "INTEGER"},
                    {"mode": "NULLABLE", "name": "campaign_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "traffic_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "external_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "target_url", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "requested_with", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "user_agent", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "owpub_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "owsubpub_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "tsp", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "owoffer_id", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "owadv_id", "type": "STRING"},
                    {
                        "name": "processing_result",
                        "type": "RECORD",
                        "mode": "REQUIRED",
                        "fields": [
                            {"mode": "REQUIRED", "name": "score", "type": "INTEGER"},
                            {"mode": "NULLABLE", "name": "fingerprint", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "redirect_url", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "token", "type": "STRING"},
                            {
                                "name": "strategies",
                                "mode": "REPEATED",
                                "type": "RECORD",
                                "fields": [
                                    {"mode": "REQUIRED", "name": "strategy", "type": "STRING"},
                                    {"mode": "REQUIRED", "name": "score", "type": "INTEGER"},
                                    {"mode": "REQUIRED", "name": "decision", "type": "STRING"},
                                    {"mode": "NULLABLE", "name": "reason", "type": "STRING"}
                                ]
                            },
                            {"mode": "REQUIRED", "name": "decision", "type": "STRING"},
                            {"mode": "REQUIRED", "name": "action", "type": "STRING"}
                        ],

                    },
                    {
                        "name": "request",
                        "type": "RECORD",
                        "mode": "REQUIRED",
                        "fields": [
                            {"mode": "REQUIRED", "name": "url", "type": "STRING"},
                            {
                                "name": "headers",
                                "type": "RECORD",
                                "mode": "REPEATED",
                                "fields": [
                                    {"mode": "REQUIRED", "name": "key", "type": "STRING"},
                                    {"mode": "NULLABLE", "name": "value", "type": "STRING"}
                                ],
                            },
                            {
                                "name": "parameters",
                                "type": "RECORD",
                                "mode": "REPEATED",
                                "fields": [
                                    {"mode": "REQUIRED", "name": "key", "type": "STRING"},
                                    {"mode": "REPEATED", "name": "value", "type": "STRING"}
                                ],
                            },
                            {"name": "remote_address", "mode": "NULLABLE", "type": "STRING"}
                        ],
                    },
                    {
                        "name": "device",
                        "type": "RECORD",
                        "mode": "REQUIRED",
                        "fields": [
                            {"mode": "NULLABLE", "name": "device_os_version", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "is_mobile", "type": "BOOLEAN"},
                            {"mode": "NULLABLE", "name": "mobile_browser", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "device_os", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "model_name", "type": "STRING"},
                            {"mode": "NULLABLE", "name": "brand_name", "type": "STRING"}
                        ],

                    },
                    {"mode": "NULLABLE", "name": "country_iso_name", "type": "STRING"},
                    {"mode": "NULLABLE", "name": "policy_id", "type": "INTEGER"},
                    {
                        "name": "whitelisting_result",
                        "mode": "REQUIRED",
                        "type": "RECORD",
                        "fields": [
                            {
                                "name": "applied_whitelists",
                                "type": "RECORD",
                                "mode": "REPEATED",
                                "fields": [
                                    {"mode": "REQUIRED", "name": "key", "type": "STRING"},
                                    {"mode": "REPEATED", "name": "value", "type": "STRING"}
                                ],
                            },
                            {"mode": "REQUIRED", "name": "is_whitelisted", "type": "BOOLEAN"}
                        ],
                    }
                ]
            },
        },
    )
