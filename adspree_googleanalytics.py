from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator, BigQueryUpsertTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from adspree.google_analytics_operator import GoogleAnalyticsToGCSOperator

GCS_BUCKET = Variable.get('airflow-bucket')
GCP_CONNECTION_ID = 'google_cloud_default'

PROJECT_NAME = Variable.get('data-project-id')
DATASET_NAME = Variable.get('adspree-dataset', default_var="dip_sandbox")
STG_TABLE = 'stg_adspree_googleanalytics'
LIVE_TABLE = 'adspree_googleanalytics'

SCHEMA_FIELDS = [
    {"name": "view_id", "type": "INTEGER"},
    {"name": "date", "type": "DATE"},
    {"name": "users", "type": "INTEGER"},
    {"name": "new_users", "type": "INTEGER"},
    {"name": "sessions", "type": "INTEGER"},
    {"name": "bounces", "type": "INTEGER"},
    #{"name": "session_duration", "type": "FLOAT"}, # TODO: doesn't seem to work
    {"name": "entrances", "type": "INTEGER"},
    {"name": "page_views", "type": "INTEGER"},
    {"name": "unique_page_views", "type": "INTEGER"},
    #{"name": "time_on_page", "type": "FLOAT"}, # TODO: doesn't seem to work
    {"name": "page_load_time", "type": "FLOAT"},
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'adspree-googleanalytics',
        default_args=default_args,
        description='Adspree',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(0),
        tags=['adspree', 'external'],
        max_active_runs=1
) as dag:
    filename = 'adspree/googleanalytics/analytics_{{ ds }}.csv'
    extract = GoogleAnalyticsToGCSOperator(
        task_id='extract_google_analytics',
        gcs_conn_id=GCP_CONNECTION_ID,
        bucket=GCS_BUCKET,
        object_name=filename,
        analytics_connection_id='adspree-google-analytics',
        date_from='{{ macros.ds_add(ds, -30) }}',
        date_to='{{ ds }}',
        dimensions=[{"name": "ga:date"}],
        metrics=[
            {"expression": "ga:users"},
            {"expression": "ga:newUsers"},
            {"expression": "ga:sessions"},
            {"expression": "ga:bounces"},
            #{"expression": "ga:sessionDuration"}, # TODO: doesn't seem to work
            {"expression": "ga:entrances"},
            {"expression": "ga:pageviews"},
            {"expression": "ga:uniquePageviews"},
            {"expression": "ga:timeOnPage"},
            {"expression": "ga:pageLoadTime"},
        ]
    )

    staging = GCSToBigQueryOperator(
        autodetect=False,
        schema_fields=[
            {"name": "view_id", "type": "INTEGER"},
            {"name": "date", "type": "INTEGER"},
            {"name": "users", "type": "INTEGER"},
            {"name": "newUsers", "type": "INTEGER"},
            {"name": "sessions", "type": "INTEGER"},
            {"name": "bounces", "type": "INTEGER"},
            #{"name": "sessionDuration", "type": "FLOAT"}, # TODO: doesn't seem to work
            {"name": "entrances", "type": "INTEGER"},
            {"name": "pageviews", "type": "INTEGER"},
            {"name": "uniquePageviews", "type": "INTEGER"},
            #{"name": "timeOnPage", "type": "FLOAT"}, # TODO: doesn't seem to work
            {"name": "pageLoadTime", "type": "FLOAT"},
        ],
        skip_leading_rows=1,
        task_id='staging',
        write_disposition='WRITE_TRUNCATE',
        destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.{STG_TABLE}',
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
            "tableReference": {"tableId": f"{LIVE_TABLE}"},
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
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{LIVE_TABLE}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.{STG_TABLE}` S
                ON T.view_id = S.view_id AND T.date = PARSE_DATE('%Y%m%d', CAST(S.date AS STRING))
                WHEN MATCHED THEN
                    UPDATE SET
                        users = S.users,
                        new_users = S.newUsers,
                        sessions = S.sessions,
                        bounces = S.bounces,
                        entrances = S.entrances,
                        page_views = S.pageviews,
                        unique_page_views = S.uniquePageviews,
                        page_load_time = S.pageLoadTime
                WHEN NOT MATCHED THEN
                    INSERT (
                        view_id, 
                        date, 
                        users, 
                        new_users, 
                        sessions, 
                        bounces,  
                        entrances, 
                        page_views, 
                        unique_page_views, 
                        page_load_time
                    )
                    VALUES (
                        S.view_id, 
                        PARSE_DATE('%Y%m%d', CAST(S.date AS STRING)), 
                        S.users, 
                        S.newUsers, 
                        S.sessions, 
                        S.bounces, 
                        S.entrances, 
                        S.pageviews, 
                        S.uniquePageviews, 
                        S.pageLoadTime
                    )
                """,
        write_disposition='WRITE_APPEND'
    )
    extract >> staging >> upsert_table >> load
