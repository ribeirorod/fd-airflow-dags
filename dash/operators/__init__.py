from typing import Any

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.ext.declarative import DeclarativeMeta

from .dash_pg import DashPartnersToPgOperator, DashUsersToPgOperator, DashBlockedPartnersToPgOperator, \
    DashBlockedLinksToPgOperator, DashAdvertisersToPgOperator, DashOffersToPgOperator
from .dash_gcs import DashToGCSOperator, DashAnalyticsToGCSOperator


class CreateMetadataTablesOperator(BaseOperator):
    def __init__(self, *, connection_id: str, meta: DeclarativeMeta, **kwargs):
        super().__init__(**kwargs)
        self.meta = meta
        self.metadata_hook = PostgresHook.get_hook(conn_id=connection_id)

    def execute(self, context: Any):
        self.meta.metadata.create_all(bind=self.metadata_hook.get_sqlalchemy_engine())
