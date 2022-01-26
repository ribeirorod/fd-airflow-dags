from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

GCP_CONNECTION_ID = 'al-bi-bq-test'

MERGE_STMT = """MERGE
INTO  `al-bi-bq-test.dwh.fact_daily_aggregated_stats` AS d
USING (
        SELECT
        DATE(partition_time) AS `date`,
        offer_id,
        advertiser_id,
        affiliate_id,
        tsp,
        country_code,
        'DASH' AS tracker,
        COUNT(transaction_id) AS clicks,
        0 AS events,
        0 AS approved_events,
        0 AS rejected_events,
        0 AS pending_events,
        0 AS revenue,
        0 AS payout,
        0 AS profit
        FROM `al-bi-bq-test.dwh.fact_clicks_v`
        WHERE DATE(partition_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
        GROUP BY 1, 2, 3, 4, 5, 6
        UNION ALL
        SELECT
        DATE(partition_time) AS `date`,
        network_offer_id AS offer_id,
        network_advertiser_id AS advertiser_id,
        network_affiliate_id AS affiliate_id,
        tsp,
        country AS country_code,
        'DASH' AS tracker,
        0 AS clicks,
        COUNT(conversion_id) AS events,
        SUM(CASE WHEN conversion_status = 'approved' THEN 1 ELSE 0 END) AS approved_events,
        SUM(CASE WHEN conversion_status = 'rejected' THEN 1 ELSE 0 END) AS rejected_events,
        SUM(CASE WHEN conversion_status = 'pending' THEN 1 ELSE 0 END) AS pending_events,
        ROUND(SUM(revenue), 2) AS revenue,
        ROUND(SUM(payout), 2) AS payout,
        ROUND(SUM(revenue) - SUM(payout), 2) AS profit
        FROM `dwh.fact_conversions_and_events_v`
        WHERE DATE(partition_time) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) AND CURRENT_DATE()
        GROUP BY 1, 2, 3, 4, 5, 6
  ) AS  v
ON ( d.`date` = v.`date`
AND  d.offer_id = v.offer_id
AND  d.advertiser_id = v.advertiser_id
AND  d.affiliate_id = v.affiliate_id
AND  d.tsp = v.tsp
AND  d.country_code = v.country_code
)
WHEN MATCHED THEN
    UPDATE SET
        clicks = v.clicks
      , events = v.events
      , approved_events = v.approved_events
      , rejected_events = v.rejected_events
      , pending_events = v.pending_events
      , revenue = v.revenue
      , payout = v.payout
      , profit = v.profit
WHEN NOT MATCHED THEN
INSERT
(`date`
, offer_id
, advertiser_id
, affiliate_id
, tsp
, country_code
, tracker
, clicks
, approved_events
, rejected_events
, pending_events
, revenue
, payout
, profit
)
VALUES( `date`
, offer_id
, advertiser_id
, affiliate_id
, tsp
, country_code
, tracker
, clicks
, approved_events
, rejected_events
, pending_events
, revenue
, payout
, profit
)"""

with DAG(
        'refresh-fact_daily_aggregated_stats',
        default_args=default_args,
        description='Refresh the daily aggregated_stas',
        schedule_interval=timedelta(days=1),
        start_date=days_ago(0),
        tags=['bq', 'fact', 'stats'],
) as dag:
    tasks = [
      BigQueryExecuteQueryOperator(
        task_id=f'merge_fact_daily_aggregated_stats',
        gcp_conn_id=GCP_CONNECTION_ID,
        use_legacy_sql=False,
        sql=MERGE_STMT,
        write_disposition='WRITE_APPEND'
        )]
