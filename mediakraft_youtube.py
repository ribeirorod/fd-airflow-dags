from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator, BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from adspree.youtube_operator import YoutubeToGCSOperator

PROJECT_NAME = Variable.get('data-project-id')
GCP_CONNECTION_ID = 'google_cloud_default'
GCP_BUCKET = Variable.get('airflow-bucket')
DATASET_NAME = Variable.get('mediakraft-dataset', default_var="dip_sandbox")
TABLE = 'content_owner_estimated_revenue_a1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
with DAG(
        'mediakraft-youtube',
        default_args=default_args,
        description='Youtube analytics',
        schedule_interval='0 0 * * *',
        start_date=days_ago(1),
        tags=['mediakraft'],
        max_active_runs=1
) as dag:
    extract = YoutubeToGCSOperator(
        task_id='extract',
        bucket=GCP_BUCKET,
        gcs_conn_id=GCP_CONNECTION_ID,
        youtube_connection_id='mediakraft-youtube',
        object_name='mediakraft/youtube/content_owner_estimated_revenue_a1_{{ ds }}.csv',
        report_type_id='content_owner_estimated_revenue_a1',
        created_after='{{ ts }}',
        content_owners=[
            {"id": "MTFYVBpznXIwKFhVJuHKZA", "name": "Mediakraft Talents"},
            {"id": "5pqBI7x7Pw5PO4lSnVZgxg", "name": "PLEXUS"},
            {"id": "YO9H9-o4YqlpjXtmY4dUZA", "name": "Mediakraft DE Originals"},
            {"id": "l_JHPVyeSJNNAtw3mwn3rw", "name": "Hometown Networks"},
            {"id": "SJBEXkK7NN253Bg0fFL5BA", "name": "Mediakraft (ComedyNet)"},
            {"id": "uCs74TCHKewfGvOQAJv2Tg", "name": "Magnolia Netz"}
        ]
    )
    staging = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        task_id=f'staging',
        write_disposition='WRITE_TRUNCATE',
        destination_project_dataset_table=f'{PROJECT_NAME}:{DATASET_NAME}.stg_{TABLE}',
        bucket=GCP_BUCKET,
        source_format='CSV',
        source_objects=['mediakraft/youtube/content_owner_estimated_revenue_a1_{{ ds }}.csv'],
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
                    {"name": "content_owner", "type": "STRING"},
                    {"name": "content_owner_id", "type": "STRING"},
                    {"name": "report_id", "type": "INTEGER"},
                    {"name": "date", "type": "DATE"},
                    {"name": "channel_id", "type": "STRING"},
                    {"name": "video_id", "type": "STRING"},
                    {"name": "claimed_status", "type": "STRING"},
                    {"name": "uploader_type", "type": "STRING"},
                    {"name": "country_code", "type": "STRING"},
                    {"name": "estimated_partner_revenue", "type": "FLOAT"},
                    {"name": "estimated_partner_ad_revenue", "type": "FLOAT"},
                    {"name": "estimated_partner_ad_auction_revenue", "type": "FLOAT"},
                    {"name": "estimated_partner_ad_reserved_revenue", "type": "FLOAT"},
                    {"name": "estimated_youtube_ad_revenue", "type": "FLOAT"},
                    {"name": "estimated_monetized_playbacks", "type": "INTEGER"},
                    {"name": "estimated_playback_based_cpm", "type": "FLOAT"},
                    {"name": "ad_impressions", "type": "INTEGER"},
                    {"name": "estimated_cpm", "type": "FLOAT"},
                    {"name": "estimated_partner_red_revenue", "type": "FLOAT"},
                    {"name": "estimated_partner_transaction_revenue", "type": "FLOAT"}
                ]
            },
        },
    )
    load = BigQueryExecuteQueryOperator(
        task_id='load',
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.{TABLE}` T
                USING `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}` S
                ON 
                    T.content_owner_id = S.content_owner_id
                    AND T.content_owner = S.content_owner
                    AND T.report_id = S.report_id
                    AND T.channel_id = S.channel_id
                    AND T.video_id = S.video_id
                    AND T.claimed_status = S.claimed_status
                    AND T.uploader_type = S.uploader_type
                    AND T.country_code = S.country_code
                    AND T.date = PARSE_DATE('%Y%m%d', CAST(S.date AS STRING))
                WHEN NOT MATCHED THEN
                INSERT (
                    content_owner,
                    content_owner_id,
                    report_id,
                    date, 
                    channel_id,
                    video_id,
                    claimed_status,
                    uploader_type,
                    country_code,
                    estimated_partner_revenue,
                    estimated_partner_ad_revenue,
                    estimated_partner_ad_auction_revenue,
                    estimated_partner_ad_reserved_revenue,
                    estimated_youtube_ad_revenue,
                    estimated_monetized_playbacks,
                    estimated_playback_based_cpm,
                    ad_impressions,
                    estimated_cpm,
                    estimated_partner_red_revenue,
                    estimated_partner_transaction_revenue
                )
                VALUES (
                    S.content_owner,
                    S.content_owner_id,
                    S.report_id,
                    PARSE_DATE('%Y%m%d', CAST(S.date AS STRING)),
                    S.channel_id,
                    S.video_id,
                    S.claimed_status,
                    S.uploader_type,
                    S.country_code,
                    S.estimated_partner_revenue,
                    S.estimated_partner_ad_revenue,
                    S.estimated_partner_ad_auction_revenue,
                    S.estimated_partner_ad_reserved_revenue,
                    S.estimated_youtube_ad_revenue,
                    S.estimated_monetized_playbacks,
                    S.estimated_playback_based_cpm,
                    S.ad_impressions,
                    S.estimated_cpm,
                    S.estimated_partner_red_revenue,
                    S.estimated_partner_transaction_revenue
                )
                """,
        write_disposition='WRITE_APPEND'
    )

    extract >> staging >> upsert_table >> load
