from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from spideraf import GCSUploadOperator

GCP_CONNECTION_ID = 'google_cloud_default'
GCP_BUCKET = Variable.get('airflow-bucket')
PROJECT_NAME = Variable.get('data-project-id')
#DATASET_NAME = Variable.get('applift-dataset', default_var="dip_sandbox")
DATASET_NAME = 'bi_applift_migration' # TODO: remove after migration
TABLE = 'stg_spideraf_upload'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}
with DAG(
        'spideraf-upload',
        default_args=default_args,
        description='Spider AF upload',
        schedule_interval='0 2 * * *',
        start_date=days_ago(1),
        tags=['spideraf', 'report', 'external'],
        max_active_runs=1
) as dag:
    filename = 'shared/spideraf/conversions_{{ ds_nodash }}_1.csv'

    query = BigQueryInsertJobOperator(
        task_id='query_data',
        location='US',
        configuration={
            "query": {
                "query": """
                    SELECT
                        date as time,
                        network_affiliate_id as external_publisher_id,
                        source_id as external_media_id,
                        c.app_id as external_site_id,
                        network_offer_id as external_sub_site_id,
                        network_offer_id as campaign_id,
                        case when cpa_enabled = 1 then'cpa' else 'cpi'end as campaign_type,
                        e.event_name as conversion_type ,
                        'clicks'as attribution_type,
                        click_date as attribution_time,
                        session_user_ip asip ,
                        case when platform = 'iOS' then idfa else google_ad_id end as device_id,
                        click_ua as user_agent,
                        isp as carrier,
                        referer as referer,
                        platform as platform,
                        handset as device_type,
                        os_version as os_version,
                        language as language,
                        is_wireless_device as wifi,
                        transaction_id as session_id
                    FROM `al-bi-bq-test.dwh.fact_conversions_and_events` as c
                        left join `al-bi-bq-test.dwh.dim_event_types` as e using (event_type)
                        left join `al-bi-bq-test.dwh.dim_offers` as o on c.network_offer_id = o.offer_id
                    WHERE 
                        DATE(_PARTITIONTIME) = '{{ ds }}' 
                        and country = 'JP'
                        and c.event_type not in ('dzlft','dy5lk','alltac')
                    ORDER BY 1 DESC
                     """,
                "destinationTable": {
                    "projectId": PROJECT_NAME,
                    "datasetId": DATASET_NAME,
                    "tableId": TABLE
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False
            }
        }
    )

    dump = BigQueryToGCSOperator(
        task_id='dump',
        source_project_dataset_table=f'{PROJECT_NAME}.{DATASET_NAME}.{TABLE}',
        destination_cloud_storage_uris=[f'gs://{GCP_BUCKET}/' + filename],
        export_format='CSV',
        field_delimiter=',',
        print_header=True,
        gcp_conn_id=GCP_CONNECTION_ID
    )

    upload = GCSUploadOperator(
        task_id='upload',
        source_bucket=GCP_BUCKET,
        source_obj=filename,
        source_gcp_connection=GCP_CONNECTION_ID,
        target_bucket='spideraf-exchange-applift',
        target_obj='data/conversions_{{ ds_nodash }}_1.csv',
        target_gcp_connection='spideraf-service-account'
    )

    query >> dump >> upload
