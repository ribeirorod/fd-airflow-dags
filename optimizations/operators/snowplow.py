import abc
import logging
from functools import cached_property
from typing import List, Dict, Any

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import sessionmaker

from optimizations.models import OptimizationTask


class SnowplowOperator(BaseOperator, metaclass=abc.ABCMeta):
    template_fields = ['query']

    def __init__(
            self,
            *,
            snowplow_connection_id: str,
            query: str,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.snowplow_connection_id = snowplow_connection_id
        self.query = query

    @cached_property
    def snowplow_hook(self):
        return PostgresHook(postgres_conn_id=self.snowplow_connection_id)

    @abc.abstractmethod
    def process_df(self, df: pd.DataFrame):
        pass

    def execute(self, context: Any):
        df = self.snowplow_hook.get_pandas_df(sql=self.query)
        print(df)
        print(df.dtypes)
        self.process_df(df)


class SnowplowToBigQueryOperator(SnowplowOperator):

    def __init__(
            self,
            *,
            gcp_conn_id: str,
            project_id: str,
            target_table: str,
            table_schema: List[Dict],
            if_exists: str = 'append',
            **kwargs
    ):
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.project_id = project_id
        self.target_table = target_table
        self.table_schema = table_schema
        self.if_exists = if_exists

    def process_df(self, df: pd.DataFrame):
        bq_hook = BigQueryHook(gcp_conn_id=self.gcp_conn_id)
        df.to_gbq(
            project_id=self.project_id,
            destination_table=self.target_table,
            table_schema=self.table_schema,
            if_exists=self.if_exists,
            credentials=bq_hook._get_credentials()
        )


class SnowplowIngestActionsOperator(SnowplowOperator):

    def __init__(self, *, postgres_connection_id: str, **kwargs):
        super().__init__(
            query="""
            SELECT id, advertiser_id,
                   publisher_id, offer_id, tsp, offer_name, publisher_name,
                   source as affpubid,
                   publisher_account_manager_name,
                   reasons as reason,
                   CASE
                       WHEN (change_type=3) THEN 'block_tsp'
                       WHEN (change_type=6) THEN 'block_affpubid'
                   END AS change_type,
                   delay_hours,
                   CASE
                       WHEN (process_status IN (1, 0) OR process_status IS NULL) THEN 'registered'
                       WHEN (process_status=2) THEN 'success'
                       WHEN (process_status=3) THEN 'failed'
                       WHEN (process_status=12) THEN 'email_success'
                       WHEN (process_status=13) THEN 'email_failed'
                       WHEN (process_status=22) THEN 'cancelled'
                    END AS status,
                   string_to_array(email_to, ',') as email_to,
                   string_to_array(email_cc, ',') as email_cc,
                   string_to_array(email_bcc, ',') as email_bcc, updated_by, created_on, updated_on
            FROM optimizations.ym_approved_actions
            WHERE created_on >= '{{ ds }}' AND process_status = 0
            """,
            **kwargs
        )
        self.postgres_connection_id = postgres_connection_id

    def process_df(self, df: pd.DataFrame):
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_connection_id)
        engine = postgres_hook.get_sqlalchemy_engine()
        orm_sessionmaker = sessionmaker(bind=engine)
        session = orm_sessionmaker()
        for i, row in df.iterrows():
            task = OptimizationTask(**row)
            instance = session.query(OptimizationTask).filter_by(id=task.id).first()
            if instance:
                logging.info("Task [%s] already exists, skipping...", task.id)
            else:
                logging.info("Adding task [%s]", task.id)
                session.add(task)
        session.commit()
