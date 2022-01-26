from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
import spideraf

SPIDERAF_CONNECTION = 'spideraf'
GCP_CONNECTION_ID = 'google_cloud_default'
GCP_BUCKET = Variable.get('airflow-bucket')
PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('applift-dataset', default_var="dip_sandbox")
STG_TABLE = 'stg_spideraf'
LIVE_TABLE = 'spideraf'

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
    {"name": "external_media_id", "type": "STRING"},
    {"name": "external_publisher_id", "type": "INTEGER"},
    {"name": "external_site_id", "type": "STRING"},
    {"name": "external_sub_site_id", "type": "STRING"},
    {"name": "impression_data", "type": "STRING"},
    {"name": "inserted_at", "type": "TIMESTAMP"},
    {"name": "name", "type": "STRING"},
    {"name": "report_id", "type": "INTEGER"},
    {"name": "score", "type": "INTEGER"},
    {"name": "site_id", "type": "INTEGER"},
    {"name": "click_data_base_count", "type": "INTEGER"},
    {"name": "click_data_count", "type": "INTEGER"},
    {"name": "click_data_givt_count", "type": "INTEGER"},
    {"name": "click_data_givt_invalid_count", "type": "INTEGER"},
    {"name": "click_data_givt_score", "type": "FLOAT"},
    {"name": "conversion_data_base_count", "type": "INTEGER"},
    {"name": "conversion_data_browser_score", "type": "FLOAT"},
    {"name": "conversion_data_count", "type": "INTEGER"},
    {"name": "conversion_data_device_model_score", "type": "FLOAT"},
    {"name": "conversion_data_flooding_score", "type": "FLOAT"},
    {"name": "conversion_data_givt_count", "type": "INTEGER"},
    {"name": "conversion_data_givt_invalid_count", "type": "INTEGER"},
    {"name": "conversion_data_givt_score", "type": "FLOAT"},
    {"name": "conversion_data_hours_score", "type": "FLOAT"},
    {"name": "conversion_data_isp_score", "type": "FLOAT"},
    {"name": "conversion_data_language_score", "type": "FLOAT"},
    {"name": "conversion_data_minutes_score", "type": "FLOAT"},
    {"name": "conversion_data_platform_platforms", "type": "STRING"},
    {"name": "conversion_data_platform_score", "type": "FLOAT"},
    {"name": "conversion_data_seconds_score", "type": "FLOAT"},
    {"name": "conversion_data_time_to_install_farm_score", "type": "FLOAT"},
    {"name": "conversion_data_time_to_install_flooding_ct_score", "type": "FLOAT"},
    {"name": "conversion_data_time_to_install_flooding_score", "type": "FLOAT"},
    {"name": "conversion_data_time_to_install_flooding_vt_score", "type": "FLOAT"},
    {"name": "conversion_data_time_to_install_hijack_score", "type": "FLOAT"},
    {"name": "feedback_classification", "type": "STRING"},
    {"name": "feedback_inserted_at", "type": "TIMESTAMP"},
    {"name": "feedback_memo", "type": "STRING"},
    {"name": "feedback_report_id", "type": "INTEGER"},
    {"name": "feedback_user_name", "type": "STRING"},
    {"name": "conversion_data_ip_score", "type": "FLOAT"},
    {"name": "conversion_data_new_device_score", "type": "FLOAT"},
    {"name": "conversion_data_carrier_score", "type": "FLOAT"},
    {"name": "conversion_data_timezone_score", "type": "FLOAT"},
    {"name": "conversion_data_uid_score", "type": "FLOAT"}
]
with DAG(
        'spideraf-reports',
        default_args=default_args,
        description='Spider AF reports',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(0),
        tags=['spideraf', 'report', 'external'],
        max_active_runs=1
) as dag:
    report_types = ['clicks_cv', 'installs', 'sites_lvl']
    for report_type in report_types:
        extract = spideraf.SpiderAfToGcs(
            task_id='extract_{}'.format(report_type),
            spideraf_connection_id=SPIDERAF_CONNECTION,
            report_type=report_type,
            schema_fields=SCHEMA_FIELDS,
            bucket=GCP_BUCKET,
            gcs_conn_id=GCP_CONNECTION_ID,
            object_name=f'spideraf/{report_type}_{{{{ ds }}}}.csv')
        staging = GCSToBigQueryOperator(
            autodetect=True,
            skip_leading_rows=1,
            task_id=f'staging_{report_type}',
            write_disposition='WRITE_TRUNCATE',
            destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.{STG_TABLE}_{report_type}',
            bucket=GCP_BUCKET,
            source_format='CSV',
            source_objects=['{{ ti.xcom_pull(key="return_value") }}'],
            bigquery_conn_id=GCP_CONNECTION_ID,
            google_cloud_storage_conn_id=GCP_CONNECTION_ID
        )
        upsert_table = BigQueryUpsertTableOperator(
            task_id=f'upsert_table_{report_type}',
            gcp_conn_id=GCP_CONNECTION_ID,
            dataset_id=DATASET_NAME,
            project_id=PROJECT_NAME,
            table_resource={
                "tableReference": {"tableId": f"{LIVE_TABLE}_{report_type}"},
                "schema": {
                    "fields": SCHEMA_FIELDS
                },
            },
        )
        load = BigQueryExecuteQueryOperator(
            task_id=f'load_{report_type}',
            gcp_conn_id=GCP_CONNECTION_ID,
            use_legacy_sql=False,
            sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{LIVE_TABLE}_{report_type}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.{STG_TABLE}_{report_type}` S
                ON T.report_id = S.report_id AND T.site_id = S.site_id AND T.inserted_at = S.inserted_at
                WHEN NOT MATCHED THEN
                INSERT (
                    {','.join([x['name'] for x in SCHEMA_FIELDS])}
                )
                VALUES (
                    S.external_media_id,
                    S.external_publisher_id,
                    CAST(S.external_site_id AS STRING),
                    CAST(S.external_sub_site_id AS STRING),
                    S.impression_data,
                    S.inserted_at,
                    S.name,
                    S.report_id,
                    S.score,
                    S.site_id,
                    CAST(S.click_data_base_count AS INTEGER),
                    CAST(S.click_data_count AS INTEGER),
                    CAST(S.click_data_givt_count AS INTEGER),
                    CAST(S.click_data_givt_invalid_count AS INTEGER),
                    CAST(S.click_data_givt_score AS FLOAT64),
                    CAST(S.conversion_data_base_count AS INTEGER),
                    S.conversion_data_browser_score,
                    CAST(S.conversion_data_count AS INTEGER),
                    S.conversion_data_device_model_score,
                    S.conversion_data_flooding_score,
                    CAST(S.conversion_data_givt_count AS INTEGER),
                    CAST(S.conversion_data_givt_invalid_count AS INTEGER),
                    S.conversion_data_givt_score,
                    S.conversion_data_hours_score,
                    S.conversion_data_isp_score,
                    S.conversion_data_language_score,
                    S.conversion_data_minutes_score,
                    S.conversion_data_platform_platforms,
                    S.conversion_data_platform_score,
                    S.conversion_data_seconds_score,
                    S.conversion_data_time_to_install_farm_score,
                    S.conversion_data_time_to_install_flooding_ct_score,
                    S.conversion_data_time_to_install_flooding_score,
                    S.conversion_data_time_to_install_flooding_vt_score,
                    S.conversion_data_time_to_install_hijack_score,
                    S.feedback_classification,
                    PARSE_TIMESTAMP('%F %T %Ez', S.feedback_inserted_at),
                    S.feedback_memo,
                    CAST(CAST(S.feedback_report_id AS FLOAT64) AS INTEGER),
                    S.feedback_user_name,
                    S.conversion_data_ip_score,
                    CAST(S.conversion_data_new_device_score AS FLOAT64),
                    CAST(S.conversion_data_carrier_score AS FLOAT64),
                    CAST(S.conversion_data_timezone_score AS FLOAT64),
                    CAST(S.conversion_data_uid_score AS FLOAT64)
                )
                """,
            write_disposition='WRITE_APPEND'
        )
        extract >> staging >> upsert_table >> load
