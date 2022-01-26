import os
from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator, \
    BigQueryDeleteTableOperator, BigQueryExecuteQueryOperator
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago

from bq import BigQueryRollupOperator
from reachhero import MysqlRollupOperator
from tesseract import ClickhouseRollupOperator

GCP_CONNECTION_ID = "google_cloud_default"
PROJECT_NAME = Variable.get('data-project-id')
MEDIAKRAFT_DATASET_NAME = Variable.get('mediakraft-dataset', default_var="dip_sandbox")
ADSPREE_DATASET = Variable.get('adspree-dataset', default_var='dip_sandbox')
DATASET_NAME = Variable.get('unity-dataset', default_var="dip_sandbox")
TABLE = 'basic_daily_report'
GCS_BUCKET = Variable.get('airflow-bucket')

TABLE_SCHEMA = [
    {"name": "year", "type": "INTEGER"},
    {"name": "month", "type": "INTEGER"},
    {"name": "day", "type": "INTEGER"},
    {"name": "value_at", "type": "DATE"},
    {"name": "product_key", "type": "STRING"},
    {"name": "revenues", "type": "FLOAT"},
    {"name": "direct_costs", "type": "FLOAT"}
]

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}
with DAG(
        'unity-daily',
        default_args=default_args,
        description='Provide data for unity project',
        schedule_interval='0 2 * * *',
        start_date=days_ago(1),
        tags=['unity', 'rollups'],
        max_active_runs=1
) as dag:

    upload_products = LocalFilesystemToGCSOperator(
        task_id="upload_products_csv",
        src=f"{os.path.abspath(os.path.dirname(__file__))}/data/unity_products.csv",
        dst='unity/products_{{ ds }}.csv',
        bucket=GCS_BUCKET,
        gcp_conn_id=GCP_CONNECTION_ID,
    )

    upsert_products_table = BigQueryUpsertTableOperator(
        task_id="upsert_products_table",
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": "products"},
            "schema": {
                "fields": [
                    {"name": "id", "type": "STRING"},
                    {"name": "name", "type": "STRING"},
                    {"name": "business_line", "type": "STRING"},
                    {"name": "business_group", "type": "STRING"},
                    {"name": "business_unit", "type": "STRING"},
                    {"name": "notes", "type": "STRING"},
                    {"name": "avg_estimated_daily_revenues", "type": "FLOAT"},
                    {"name": "avg_estimated_daily_direct_costs", "type": "FLOAT"},
                ]
            },
        },
    )

    load_products = GCSToBigQueryOperator(
        autodetect=True,
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        task_id="load_products",
        write_disposition="WRITE_TRUNCATE",
        destination_project_dataset_table=f"{PROJECT_NAME}:{DATASET_NAME}.products",
        bucket=GCS_BUCKET,
        source_format="CSV",
        source_objects=['unity/products_{{ ds }}.csv'],
        bigquery_conn_id=GCP_CONNECTION_ID,
        google_cloud_storage_conn_id=GCP_CONNECTION_ID,
    )

    clean_stg_table = BigQueryDeleteTableOperator(
        task_id="clean_stg_table",
        ignore_if_missing=True,
        deletion_dataset_table=f"{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}"
    )

    upsert_staging_table = BigQueryUpsertTableOperator(
        task_id=f'upsert_stg_table',
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": f'stg_{TABLE}'},
            "schema": {
                "fields": TABLE_SCHEMA
            },
        },
    )

    mediakraft_data = BigQueryRollupOperator(
        task_id='mediakraft_data',
        gcp_conn_id=GCP_CONNECTION_ID,
        query=f"""
                    SELECT 
                        CAST(FORMAT_DATE("%Y", d.date) AS INT64) AS year,
                        CAST(FORMAT_DATE("%Y%m", d.date) AS INT64) AS month,
                        CAST(FORMAT_DATE("%Y%m%d", d.date) AS INT64) AS day,
                        CAST(d.date AS DATE) AS value_at,
                        CONCAT('MK-yt-', d.content_owner_id) AS product_key,
                        ROUND(SUM(CASE 
                            WHEN content_owner_id = 'DIYrC9g4HIz_T7EJuEZ7YA' 
                                THEN estimated_partner_revenue 
                            ELSE estimated_youtube_ad_revenue 
                            END) * 0.84114900954704130420225, 2
                        ) AS revenues,
                        ROUND(SUM(CASE
                            WHEN content_owner_id IN ('T6YRWhYberPdRbf3Z5rbQ', 'YO9H9-o4YqlpjXtmY4dUZA', 'FQX78HL5L6oCjJd3ejmFYw')
                                THEN estimated_youtube_ad_revenue - estimated_partner_revenue
                            WHEN content_owner_id = 'DIYrC9g4HIz_T7EJuEZ7YA'
                                THEN estimated_partner_revenue * 0.18
                            ELSE estimated_youtube_ad_revenue * 0.9
                            END) * 0.84114900954704130420225 , 2
                        )  AS direct_costs
                        FROM {PROJECT_NAME}.{MEDIAKRAFT_DATASET_NAME}.content_owner_estimated_revenue_a1 d
                        WHERE d.date BETWEEN '{{{{ macros.ds_add(ds, -7) }}}}' AND '{{{{ macros.ds_add(ds, 1) }}}}'
                        GROUP BY d.content_owner_id, d.date
                     """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )
    applift_data = BigQueryRollupOperator(
        task_id='applift_data',
        gcp_conn_id=GCP_CONNECTION_ID,
        query="""
                SELECT 
                CAST(FORMAT_DATE("%Y"    , e.date) AS INT64) AS year
                , CAST(FORMAT_DATE("%Y%m"  , e.date) AS INT64) AS month
                , CAST(FORMAT_DATE("%Y%m%d", e.date) AS INT64) AS day
                , CAST(e.date AS DATE) AS value_at
                , 'AL-total' AS product_key
                , ROUND(sum(revenue*0.85*0.92), 2) AS revenue -- * USD/EUR rate * discount rate
                , ROUND(sum(payout*0.85*0.92), 2) as payout-- * USD/EUR rate * discount rate
                FROM `al-bi-bq-test.dwh.fact_conversions_and_events_v` as e
                WHERE date(partition_time) IN ('{{ ds }}')
                GROUP BY
                    value_at, year, month, day
                ORDER BY value_at
            """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    core_data = ClickhouseRollupOperator(
        task_id='core_data',
        gcp_conn_id=GCP_CONNECTION_ID,
        clickhouse_conn_id='clickhouse_default',
        query="""
                SELECT toYear(toDate(f.timestamp, 'Europe/Berlin'))                                                       AS "year",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 100 + toMonth(toDate(f.timestamp, 'Europe/Berlin')) AS "month",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 10000 + toMonth(f.timestamp) * 100 +
                       toDayOfMonth(toDate(f.timestamp, 'Europe/Berlin'))                                                 AS "day",
                       CAST(toDate(f.timestamp, 'Europe/Berlin') AS Date)                                                 AS value_at,
                       concat('MED-core-', replace(f.country, 'GB', 'UK'))                                                AS product_key,
                       sum(ifNull(f.net_amount_eur, 0))                                                                   AS revenues,
                       sum(ifNull(f.publisher_cost_eur, 0))                                                               AS direct_costs
                FROM fact f
                WHERE f.instance = 'core'
                  AND toDate(f.timestamp, 'Europe/Berlin') >= '{{ ds }}'
                  AND toDate(f.timestamp, 'Europe/Berlin') < '{{ macros.ds_add(ds, 1) }}'
                  AND (f.raw_revenue_amount_eur IS NOT NULL OR f.gec_eur IS NOT NULL OR f.net_amount_eur IS NOT NULL OR
                       f.publisher_cost_eur IS NOT NULL)
                  AND ifNull(f.pid, -1) NOT IN ( -- Exclude RevShare partner
                                                2509, -- FFM
                                                3052, -- emobi
                                                2885, -- Telefuture
                                                2952, -- Creative Clicks
                                                2825, -- First Mobile Cash
                                                3156, -- Jestoro
                                                3251, -- Davamobi
                                                3312 -- Belive Mobile
                    )
                GROUP BY "year", "month", "day", "value_at", "product_key"
        """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    core_revshare_data = ClickhouseRollupOperator(
        task_id='core_revshare_data',
        gcp_conn_id=GCP_CONNECTION_ID,
        clickhouse_conn_id='clickhouse_default',
        query="""
                SELECT toYear(toDate(f.timestamp, 'Europe/Berlin'))                                                       AS "year",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 100 + toMonth(toDate(f.timestamp, 'Europe/Berlin')) AS "month",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 10000 + toMonth(f.timestamp) * 100 +
                       toDayOfMonth(toDate(f.timestamp, 'Europe/Berlin'))                                                 AS "day",
                       CAST(toDate(f.timestamp, 'Europe/Berlin') AS Date)                                                 AS value_at,
                       'MED-revshare'                                                                                     AS product_key,
                       sum(ifNull(f.net_amount_eur, 0))                                                                   AS revenues,
                       sum(ifNull(f.net_amount_eur, 0)
                           * CASE ifNull(f.pid, -1)
                                 WHEN 2509 THEN 0.85 -- FFM
                                 WHEN 3052 THEN 0.85 -- emobi
                                 WHEN 2885 THEN 0.75 -- Telefuture
                                 WHEN 2952 THEN 0.88 -- Creative Clicks
                                 WHEN 2825 THEN 0.85 -- First Mobile Cash
                                 WHEN 3156 THEN 0.70 -- Jestoro
                                 WHEN 3251 THEN 0.75 -- Davamobi
                                 WHEN 3312 THEN 0.80 -- Belive Mobile
                                 ELSE 1
                               END
                           )
                           +
                       ifNull(sum(f.publisher_cost_eur), 0)                                                               AS direct_costs
                FROM fact f
                WHERE f.instance = 'core'
                  AND toDate(f.timestamp, 'Europe/Berlin') >= '{{ ds }}'
                  AND toDate(f.timestamp, 'Europe/Berlin') < '{{ macros.ds_add(ds, 1) }}'
                  AND (f.raw_revenue_amount_eur IS NOT NULL OR f.gec_eur IS NOT NULL OR f.net_amount_eur IS NOT NULL OR
                       f.publisher_cost_eur IS NOT NULL)
                  AND ifNull(f.pid, -1) IN ( -- Include RevShare partner
                                            2509, -- FFM
                                            3052, -- emobi
                                            2885, -- Telefuture
                                            2952, -- Creative Clicks
                                            2825, -- First Mobile Cash
                                            3156, -- Jestoro
                                            3251, -- Davamobi
                                            3312 -- Belive Mobile
                    )
                GROUP BY "year", "month", "day", "value_at", "product_key"
        """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    sureyield_data = ClickhouseRollupOperator(
        task_id='sureyield_data',
        gcp_conn_id=GCP_CONNECTION_ID,
        clickhouse_conn_id='clickhouse_default',
        query="""
                SELECT toYear(toDate(f.timestamp, 'Europe/Berlin'))                                                       AS "year",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 100 + toMonth(toDate(f.timestamp, 'Europe/Berlin')) AS "month",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 10000 + toMonth(f.timestamp) * 100 +
                       toDayOfMonth(toDate(f.timestamp, 'Europe/Berlin'))                                                 AS "day",
                       CAST(toDate(f.timestamp, 'Europe/Berlin') AS Date)                                                 AS value_at,
                       'MED-perfmkt-sureyield'                                                                            AS product_key,
                       sum(f.raw_revenue_amount_eur)                                                                      AS revenues,
                       sum(f.publisher_cost_eur)                                                                          AS direct_costs
                FROM fact f
                WHERE f.instance = 'sureyield'
                  AND toDate(f.timestamp, 'Europe/Berlin') >= '{{ ds }}'
                  AND toDate(f.timestamp, 'Europe/Berlin') < '{{ macros.ds_add(ds, 1) }}'
                  AND (f.raw_revenue_amount_eur IS NOT NULL OR f.publisher_cost_eur IS NOT NULL)
                GROUP BY "year", "month", "day", "value_at", "product_key"
        """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    adspree_data = ClickhouseRollupOperator(
        task_id='adspree_data',
        gcp_conn_id=GCP_CONNECTION_ID,
        clickhouse_conn_id='clickhouse_default',
        query="""
                SELECT toYear(toDate(f.timestamp, 'Europe/Berlin'))                                                       AS "year",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 100 + toMonth(toDate(f.timestamp, 'Europe/Berlin')) AS "month",
                       toYear(toDate(f.timestamp, 'Europe/Berlin')) * 10000 + toMonth(f.timestamp) * 100 +
                       toDayOfMonth(toDate(f.timestamp, 'Europe/Berlin'))                                                 AS "day",
                       CAST(toDate(f.timestamp, 'Europe/Berlin') AS Date)                                                 AS value_at,
                       CASE
                           WHEN ifNull(pid, partner_id) NOT IN
                                (121, 193, 198, 339, 120, 122, 123, 261, 262, 263, 264, 265, 266, 267, 268, 269, 270, 278, 279, 280,
                                 281, 282, 299, 300, 305,
                                 307, 322, 323, 324, 325, 326, 327)
                               THEN 'SY-as-third-party-supply'
                           ELSE 'AS-others'
                           END                                                                                            AS product_key,
                       SUM(ifNull(raw_revenue_amount_eur, 0))                                                             AS revenues,
                       SUM(ifNull(publisher_cost_eur, 0))                                                                 AS direct_costs
                From fact f
                WHERE instance = 'adspree'
                  AND toDate(f.timestamp, 'Europe/Berlin') >= '{{ ds }}'
                  AND toDate(f.timestamp, 'Europe/Berlin') < '{{ macros.ds_add(ds, 1) }}'
                  AND (f.raw_revenue_amount_eur IS NOT NULL OR f.publisher_cost_eur IS NOT NULL)
                GROUP BY "year", "month", "day", "value_at", "product_key"
        """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    reachhero_marketplace_data = MysqlRollupOperator(
        task_id='reachhero_marketplace_data',
        mysql_conn_id='reachhero-db-connection',
        gcp_conn_id=GCP_CONNECTION_ID,
        query="""
            SELECT 
               CAST(DATE_FORMAT(DATE('{{ ds }}'), '%Y') AS UNSIGNED)     AS "year",
               CAST(DATE_FORMAT(DATE('{{ ds }}'), '%Y%m') AS UNSIGNED)   AS "month",
               CAST(DATE_FORMAT(DATE('{{ ds }}'), '%Y%m%d') AS UNSIGNED) AS "day",
               DATE_FORMAT(DATE('{{ ds }}'), '%Y-%m-%d')                 AS "value_at",
               'RH-marketplace-total'                             AS "product_key",
               ROUND(SUM(revenues / allocation), 2)               AS "revenues",
               ROUND(SUM((revenues * .7) / allocation), 2)        AS "direct_costs"
            FROM (
                -- New Money
                select ii.total / 100 AS revenues,
                ( -- InfluencerPayout
                 CASE
                     WHEN (select distinct iit.campaign_id is not null from invoice_item iit where iit.invoice_id = ii.id)
                         THEN
                         (CASE
                              WHEN (select cr.name
                                    from campaign_recipe cr
                                    where id = (select cf.recipe_id
                                                from campaign_features cf
                                                where cf.campaign_id = (select distinct campaign_id
                                                                        from invoice_item iit
                                                                        where iit.invoice_id = ii.id))) <> 'giveaway'
                                  THEN (convert((select sum(iit.price / 100)
                                                 from invoice_item iit
                                                 where iit.invoice_id = ii.id), decimal(12, 2))) - (convert(
                                              (select sum(iit.price / 100)
                                               from invoice_item iit
                                               where iit.invoice_id = ii.id) * (select csp.influencer_provision
                                                                                from campaign_service_provision csp
                                                                                where csp.campaign_id =
                                                                                      (select distinct campaign_id
                                                                                       from invoice_item iit
                                                                                       where iit.invoice_id = ii.id)) /
                                              100, decimal(12, 2)))
                              ELSE convert(coalesce(0, 0), decimal(12, 2))
                             END)
                     ELSE convert(coalesce(0, 0), decimal(12, 2))
                     END
                 ) AS direct_costs,
                CASE
                    WHEN period_end > LAST_DAY(ii.created_at)
                        THEN 1 + TIMESTAMPDIFF(MONTH
                        , ADDDATE(LAST_DAY(SUBDATE((CASE
                                                        WHEN coalesce(period_start, created_at) <= created_at
                                                            THEN created_at
                                                        ELSE period_start END), INTERVAL 1 MONTH)), 1)
                        , last_day(CASE
                                       WHEN coalesce(period_end, created_at) <= created_at THEN created_at
                                       ELSE period_end END)
                        )
                    ELSE 1
                    END           allocation
                FROM invoice_invoice ii
                WHERE ii.fastbill_invoice_url is not null
                AND ii.created_at >= '{{ ds }}'
                AND ii.created_at < ADDDATE(DATE('{{ ds }}'), INTERVAL 1 DAY)
                AND (ii.period_start < LAST_DAY(ii.created_at) OR ii.period_start IS NULL) -- Exclude future money
                AND (ii.canceled_at is null OR ii.canceled_at > last_day(DATE('{{ ds }}')))
            UNION ALL
            -- Old money
            SELECT ii.total / 100 AS revenues,
                (
                    -- InfluencerPayout
                    CASE
                        WHEN (select distinct iit.campaign_id is not null
                              from invoice_item iit
                              where iit.invoice_id = ii.id)
                            THEN
                            (CASE
                                 WHEN (select cr.name
                                       from campaign_recipe cr
                                       where id = (select cf.recipe_id
                                                   from campaign_features cf
                                                   where cf.campaign_id = (select distinct campaign_id
                                                                           from invoice_item iit
                                                                           where iit.invoice_id = ii.id))) <> 'giveaway'
                                     THEN (convert((select sum(iit.price / 100)
                                                    from invoice_item iit
                                                    where iit.invoice_id = ii.id), decimal(12, 2))) - (convert(
                                                 (select sum(iit.price / 100)
                                                  from invoice_item iit
                                                  where iit.invoice_id = ii.id) * (select csp.influencer_provision
                                                                                   from campaign_service_provision csp
                                                                                   where csp.campaign_id =
                                                                                         (select distinct campaign_id
                                                                                          from invoice_item iit
                                                                                          where iit.invoice_id = ii.id)) /
                                                 100, decimal(12, 2)))
                                 ELSE convert(0, decimal(12, 2))
                                END)
                        ELSE convert(0, decimal(12, 2))
                        END
                    )                                   AS direct_costs,
                (1 + TIMESTAMPDIFF(MONTH
                    , ADDDATE(LAST_DAY(SUBDATE((CASE
                                                    WHEN coalesce(period_start, created_at) <= created_at
                                                        THEN created_at
                                                    ELSE period_start END), INTERVAL 1 MONTH)), 1)
                    , last_day(CASE
                                   WHEN coalesce(period_end, created_at) <= created_at THEN created_at
                                   ELSE period_end END)
                    )
                    ) * DAYOFMONTH(last_day(DATE('{{ ds }}'))) AS allocation
            FROM invoice_invoice ii
            WHERE ii.fastbill_invoice_url is not null
               AND ii.created_at < ADDDATE(LAST_DAY(SUBDATE(DATE('{{ ds }}'), INTERVAL 1 MONTH)), 1) -- from previous month
               AND ii.period_start <= LAST_DAY(DATE('{{ ds }}'))                                     -- Period of service must start before end of the month
               AND ii.period_end >=
                   ADDDATE(LAST_DAY(SUBDATE(DATE('{{ ds }}'), INTERVAL 1 MONTH)), 1)                 -- and end after the beginning of the month
               AND (ii.canceled_at is null OR ii.canceled_at > last_day(DATE('{{ ds }}')))
            ) AS marketplace
            GROUP BY DATE('{{ ds }}')
        """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    reachhero_agency_data = MysqlRollupOperator(
        task_id='reachhero_agency_data',
        mysql_conn_id='reachhero-db-connection',
        gcp_conn_id=GCP_CONNECTION_ID,
        query="""
            SELECT 
                CAST(DATE_FORMAT(DATE('{{ ds }}'), '%Y') AS UNSIGNED)     AS "year",
                CAST(DATE_FORMAT(DATE('{{ ds }}'), '%Y%m') AS UNSIGNED)   AS "month",
                CAST(DATE_FORMAT(DATE('{{ ds }}'), '%Y%m%d') AS UNSIGNED) AS "day",
                '{{ ds }}'                AS "value_at",
                'RH-agency-total'                                  AS "product_key",
                ROUND(SUM(revenues / allocation), 2)               AS "revenues",
                ROUND(SUM((revenues * .7) / allocation), 2)        AS "direct_costs"
            FROM (
                -- New Money
                SELECT convert(i.total_cost, decimal(12, 2)) as revenues,
                (
                    /*InfluencerPayout*/
                        convert(coalesce((select sum((ii.price * ii.service_fee_enabled))
                                          from invoicing_influencer ii
                                          where ii.item_id = i.id), 0) - coalesce(
                                        (select sum((ii.price * ii.service_fee_enabled * 0.15))
                                         from invoicing_influencer ii
                                         where ii.item_id = i.id), 0), decimal(12, 2))
                        +
                        /*TalentPayout*/
                        convert(coalesce((select sum(ii.price) from invoicing_influencer ii where ii.item_id = i.id),
                                         0) -
                                coalesce((select sum((ii.price * ii.service_fee_enabled))
                                          from invoicing_influencer ii
                                          where ii.item_id = i.id), 0) - coalesce(
                                        (select sum((ii.price * ii.management_fee / 100))
                                         from invoicing_influencer ii
                                         where ii.item_id = i.id), 0), decimal(12, 2))
                    )                                 as direct_costs,
                CASE
                    WHEN end_at > LAST_DAY(i.created_at)
                        THEN 1 + TIMESTAMPDIFF(MONTH
                        , ADDDATE(LAST_DAY(SUBDATE((CASE
                                                        WHEN coalesce(start_at, created_at) <= created_at
                                                            THEN created_at
                                                        ELSE start_at END), INTERVAL 1 MONTH)), 1)
                        , last_day(CASE WHEN coalesce(end_at, created_at) <= created_at THEN created_at ELSE end_at END)
                        )
                    ELSE 1
                    END allocation
                FROM invoicing i
                WHERE i.fastbill_invoice_number is not null
                AND i.fastbill_invoice_created_at >= '{{ ds }}'
                AND i.fastbill_invoice_created_at < ADDDATE(DATE('{{ ds }}'), INTERVAL 1 DAY)
                AND (i.start_at < LAST_DAY(i.fastbill_invoice_created_at) OR start_at IS NULL) -- Exclude future money
                AND NOT EXISTS(SELECT 1
                      FROM invoice i2
                      WHERE i.id = i2.individual_campaign_id
                      AND i2.cancelled_at BETWEEN ADDDATE(LAST_DAY(SUBDATE(DATE('{{ ds }}'), INTERVAL 1 MONTH)), 1) AND LAST_DAY(DATE('{{ ds }}')))
            UNION ALL
            -- Old money
            SELECT convert(i.total_cost, decimal(12, 2)) as  revenues,
            (
             /*InfluencerPayout*/
                 convert(coalesce((select sum((ii.price * ii.service_fee_enabled))
                                   from invoicing_influencer ii
                                   where ii.item_id = i.id), 0) - coalesce(
                                 (select sum((ii.price * ii.service_fee_enabled * 0.15))
                                  from invoicing_influencer ii
                                  where ii.item_id = i.id), 0), decimal(12, 2))
                 +
                 /*TalentPayout*/
                 convert(coalesce((select sum(ii.price) from invoicing_influencer ii where ii.item_id = i.id), 0) -
                         coalesce((select sum((ii.price * ii.service_fee_enabled))
                                   from invoicing_influencer ii
                                   where ii.item_id = i.id), 0) - coalesce(
                                 (select sum((ii.price * ii.management_fee / 100))
                                  from invoicing_influencer ii
                                  where ii.item_id = i.id), 0), decimal(12, 2))
             ) AS direct_costs, 
             CASE
                    WHEN end_at > LAST_DAY(i.created_at)
                        THEN 1 + TIMESTAMPDIFF(MONTH
                        , ADDDATE(LAST_DAY(SUBDATE((CASE
                                                        WHEN coalesce(start_at, created_at) <= created_at
                                                            THEN created_at
                                                        ELSE start_at END), INTERVAL 1 MONTH)), 1)
                        , last_day(CASE WHEN coalesce(end_at, created_at) <= created_at THEN created_at ELSE end_at END)
                        )
                    ELSE 1
                    END * DAYOFMONTH(last_day(DATE('{{ ds }}'))) allocation
            FROM invoicing i
            WHERE i.fastbill_invoice_number is not null
            AND i.fastbill_invoice_created_at <
               ADDDATE(LAST_DAY(SUBDATE(DATE('{{ ds }}'), INTERVAL 1 MONTH)), 1) -- from previous month
            AND i.start_at <= LAST_DAY(DATE('{{ ds }}'))                          -- Period of service must start before end of the month
            AND i.end_at >=
               ADDDATE(LAST_DAY(SUBDATE(DATE('{{ ds }}'), INTERVAL 1 MONTH)), 1) -- and end after the beginning of the month
            AND NOT EXISTS(SELECT 1
                          FROM invoice i2
                          WHERE i.id = i2.individual_campaign_id
                            AND i2.cancelled_at BETWEEN ADDDATE(LAST_DAY(SUBDATE(DATE('{{ ds }}'), INTERVAL 1 MONTH)), 1) AND LAST_DAY(DATE('{{ ds }}')))
        ) AS agency
        GROUP BY DATE('{{ ds }}')
        """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    adspree_sharepoint_data = BigQueryRollupOperator(
        task_id='adspree_sharepoint_data',
        gcp_conn_id=GCP_CONNECTION_ID,
        query=f"""
                WITH 
                    time_series AS (SELECT daily as value_date FROM UNNEST (GENERATE_DATE_ARRAY(DATE_TRUNC('{{{{ ds }}}}', YEAR), '{{{{ ds }}}}', INTERVAL 1 DAY)) as daily),
                    products    AS (SELECT DISTINCT REGEXP_REPLACE(CONCAT('AS', REPLACE(SUBSTR(LOWER(product_name),5),'-',' ')),'\\\s+','-') as product_key FROM `{PROJECT_NAME}.{ADSPREE_DATASET}.adspree_sharepoint_revenue` 
                                    WHERE product_name not in ('0406 - Third Party Supply','0407 - Portal - Performance')),
                    data        AS (
                                    SELECT 
                                        CAST(FORMAT_DATE("%Y%m" , EXTRACT(DATE FROM booking_period_date)) AS INT64) AS month
                                        , REGEXP_REPLACE(CONCAT('AS', REPLACE(SUBSTR(LOWER(product_name),5),'-',' ')),'\\\s+','-') as product_key
                                        , SUM (revenue) as revenues 
                                        , 0 as costs
                                    FROM `{PROJECT_NAME}.{ADSPREE_DATASET}.adspree_sharepoint_revenue`
                                    WHERE product_name not in ('0406 - Third Party Supply','0407 - Portal - Performance')
                                    GROUP BY month, product_name
                                    UNION ALL 
                                    SELECT 
                                        CAST(FORMAT_DATE("%Y%m" , EXTRACT(DATE FROM booking_period_date)) AS INT64) AS month
                                        , REGEXP_REPLACE(CONCAT('AS', REPLACE(SUBSTR(LOWER(product_name),5),'-',' ')),'\\\s+','-') as product_key
                                        , 0 as revenues 
                                        , SUM(cost) as costs
                                    FROM `{PROJECT_NAME}.{ADSPREE_DATASET}.adspree_sharepoint_cost`
                                    WHERE product_name not in ('0406 - Third Party Supply','0407 - Portal - Performance')
                                    GROUP BY month, product_name
                                    )
                    SELECT
                        CAST(FORMAT_DATE("%Y"    , value_date) AS INT64) AS year
                        , CAST(FORMAT_DATE("%Y%m"  , value_date) AS INT64) AS month
                        , CAST(FORMAT_DATE("%Y%m%d", value_date) AS INT64) AS day
                        , CAST(value_date AS DATE) AS value_at
                        , product_key

                        -- current's month: divides revenue by numbers of days in current month 
                        , CASE WHEN DATE_TRUNC(value_date, MONTH)>= DATE_TRUNC('{{{{ ds }}}}', MONTH) THEN 
                                -- current month is missing input data and requires previous period average
                                CASE WHEN SUM(IFNULL(revenues,0)) = 0 THEN
                                    (
                                        SELECT 
                                            SUM(data.revenues)/EXTRACT( DAY FROM DATE_SUB(DATE_TRUNC('{{{{ ds }}}}',MONTH), INTERVAL 1 DAY))/(select count (distinct products.product_key) from products )
                                        FROM data 
                                        WHERE data.product_key = product_key
                                        AND data.month = CAST(FORMAT_DATE("%Y%m"  , DATE_SUB('{{{{ ds }}}}', INTERVAL 1 MONTH)) AS INT64) 
                                    )
                                ELSE 
                                    ROUND(SUM(revenues) / EXTRACT( DAY FROM DATE('{{{{ ds }}}}')),2) END
                        ELSE 
                            ROUND(SUM(revenues) / EXTRACT( DAY FROM LAST_DAY(value_date, MONTH)),2) END  AS revenues
                            
                        , CASE WHEN DATE_TRUNC(value_date, MONTH)>= DATE_TRUNC('{{{{ ds }}}}', MONTH) THEN 
                                CASE WHEN SUM(IFNULL(costs,0)) = 0 THEN
                                    (
                                        SELECT 
                                            SUM(data.costs)/EXTRACT( DAY FROM DATE_SUB(DATE_TRUNC('{{{{ ds }}}}',MONTH), INTERVAL 1 DAY))/(select count (distinct products.product_key) from products ) 
                                        FROM data 
                                        WHERE data.product_key = product_key
                                        AND data.month = CAST(FORMAT_DATE("%Y%m"  , DATE_SUB('{{{{ ds }}}}', INTERVAL 1 MONTH)) AS INT64) 
                                    )
                                ELSE 
                                ROUND(SUM(costs) / EXTRACT( DAY FROM DATE('{{{{ ds }}}}')),2) END
                        ELSE 
                            ROUND(SUM(costs) / EXTRACT( DAY FROM LAST_DAY(value_date, MONTH)),2) END  AS direct_costs
                    FROM  
                        (
                            SELECT time_series.*, products.* , data.* EXCEPT (product_key)  
                            FROM
                            time_series CROSS JOIN products
                            LEFT JOIN data ON data.month = CAST(FORMAT_DATE("%Y%m" , time_series.value_date) AS INT64)  AND products.product_key = data.product_key
                            ORDER BY time_series.value_date DESC)
                    GROUP BY year, month, day, value_at, product_key
                    ORDER BY day DESC
                """,
        target_table=f"{DATASET_NAME}.stg_{TABLE}",
        project_id=PROJECT_NAME,
        table_schema=TABLE_SCHEMA
    )

    upsert_table = BigQueryUpsertTableOperator(
        task_id=f'upsert_table',
        gcp_conn_id=GCP_CONNECTION_ID,
        dataset_id=DATASET_NAME,
        project_id=PROJECT_NAME,
        table_resource={
            "tableReference": {"tableId": TABLE},
            "schema": {
                "fields": TABLE_SCHEMA
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
                    T.year = S.year
                    AND T.month = S.month
                    AND T.day = S.day
                    AND T.value_at = S.value_at
                    AND T.product_key = S.product_key
                WHEN MATCHED THEN 
                    UPDATE SET
                        revenues = S.revenues,
                        direct_costs = S.direct_costs
                WHEN NOT MATCHED THEN
                INSERT (
                    year,
                    month,
                    day,
                    value_at, 
                    product_key,
                    revenues,
                    direct_costs
                )
                VALUES (
                    S.year,
                    S.month,
                    S.day,
                    S.value_at, 
                    S.product_key,
                    S.revenues,
                    S.direct_costs
                )
                """,
        write_disposition='WRITE_APPEND'
    )

    hardcoded_estimates = BigQueryExecuteQueryOperator(
        task_id='hardcoded_estimates',
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=f"""
                MERGE `{PROJECT_NAME}.{DATASET_NAME}.stg_{TABLE}` T
                USING (
                    SELECT 
                        {{{{ macros.ds_format(ds, "%Y-%m-%d", "%Y") }}}} AS year,
                        {{{{ macros.ds_format(ds, "%Y-%m-%d", "%Y%m") }}}} AS month,
                        {{{{ ds_nodash }}}} AS day,
                        DATE('{{{{ ds }}}}') AS value_at,
                        p.id AS product_key,
                        p.avg_estimated_daily_revenues AS revenues,
                        p.avg_estimated_daily_direct_costs AS direct_costs
                    FROM `{PROJECT_NAME}.{DATASET_NAME}.products` p
                    WHERE p.avg_estimated_daily_revenues IS NOT NULL
                ) S
                ON 
                    T.year = S.year
                    AND T.month = S.month
                    AND T.day = S.day
                    AND T.value_at = S.value_at
                    AND T.product_key = S.product_key
                WHEN NOT MATCHED THEN
                INSERT (
                    year,
                    month,
                    day,
                    value_at, 
                    product_key,
                    revenues,
                    direct_costs
                )
                VALUES (
                    S.year,
                    S.month,
                    S.day,
                    S.value_at, 
                    S.product_key,
                    S.revenues,
                    S.direct_costs
                )
                """,
        write_disposition='WRITE_APPEND'
    )

    clean_stg_table >> upsert_staging_table >> [
        reachhero_agency_data,
        reachhero_marketplace_data,
        core_data,
        core_revshare_data,
        sureyield_data,
        adspree_data,
        applift_data,
        mediakraft_data,
        adspree_sharepoint_data
    ] >> hardcoded_estimates >> upsert_table >> load
    upload_products >> upsert_products_table >> load_products >> hardcoded_estimates
