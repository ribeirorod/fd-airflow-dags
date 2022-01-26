from typing import Any

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from optimizations.models import OptimizationsMetadataBase


class CreateTables(BaseOperator):
    def __init__(self, *, connection_id: str, **kwargs):
        super().__init__(**kwargs)
        self.db_hook = PostgresHook.get_hook(conn_id=connection_id)

    def execute(self, context: Any):
        OptimizationsMetadataBase.metadata.create_all(bind=self.db_hook.get_sqlalchemy_engine())