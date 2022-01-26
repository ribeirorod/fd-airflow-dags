from datetime import timedelta

from airflow import DAG

from optimizations.operators import SendEmailsOptimizationsOperator, \
    BlockPublishersOffersOptimizationsOperator, BlockLinksOptimizationsOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow.utils.dates import days_ago

from optimizations.operators.snowplow import SnowplowIngestActionsOperator

DATABASE_CONNECTION = 'operations'
SNOWPLOW_CONNECTION = 'snowplow'
DASH_CONNECTION = 'dash-applift'

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
        'applift-optimizations',
        default_args=default_args,
        description='',
        schedule_interval='*/15 * * * *',
        start_date=days_ago(1),
        tags=['optimizations', 'applift'],
        catchup=False
) as dag:
    create_tables = PostgresOperator(
        task_id='create_tables',
        postgres_conn_id=DATABASE_CONNECTION,
        sql="""
            create schema if not exists optimizations;
            create table if not exists optimizations.optimizations_tasks
            (
                id serial not null primary key,
                advertiser_id integer,
                offer_id integer,
                offer_name text,
                publisher_id integer,
                publisher_name text,
                affpubid text,
                publisher_account_manager_name text,
                reason text,
                change_type text,
                delay_hours integer,
                status text,
                email_to text[],
                email_cc text[],
                email_bcc text[],
                created_on timestamp with time zone,
                updated_on timestamp with time zone,
                email_sent_on timestamp with time zone,
                updated_by text,
                tsp integer
            );
            create table if not exists optimizations.optimizations_log
            (
                id serial not null primary key,
                event_type text,
                task_id integer,
                message text,
                created_on timestamp with time zone
            );
            """
    )

    ingest = SnowplowIngestActionsOperator(
        task_id='ingest_snowplow_tasks',
        snowplow_connection_id=SNOWPLOW_CONNECTION,
        postgres_connection_id=DATABASE_CONNECTION
    )

    emails = SendEmailsOptimizationsOperator(
        task_id='send_emails',
        db_connection_id=DATABASE_CONNECTION
    )

    block_publishers = BlockPublishersOffersOptimizationsOperator(
        task_id='block_publishers',
        db_connection_id=DATABASE_CONNECTION,
        dash_connection_id=DASH_CONNECTION
    )
    block_links = BlockLinksOptimizationsOperator(
        task_id='block_links',
        db_connection_id=DATABASE_CONNECTION,
        dash_connection_id=DASH_CONNECTION
    )

    create_tables >> ingest >> emails >> [block_publishers, block_links]
