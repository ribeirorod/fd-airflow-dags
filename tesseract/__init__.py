from typing import Any, List, Optional, Dict, Union, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow_clickhouse_plugin.hooks.clickhouse_hook import ClickHouseHook


class ClickhouseRollupOperator(BaseOperator):
    template_fields = ["query"]

    def __init__(
            self,
            *,
            clickhouse_conn_id: str,
            gcp_conn_id: str,
            query: str,
            target_table: str,
            table_schema: List[Dict],
            project_id: str,
            delegate_to: Optional[str] = None,
            location: Optional[str] = None,
            impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
            **kwargs):
        super().__init__(**kwargs)
        self.clickhouse_conn_id = clickhouse_conn_id
        self.query = query
        self.gcp_conn_id = gcp_conn_id
        self.query = query
        self.project_id = project_id
        self.target_table = target_table
        self.table_schema = table_schema
        self.delegate_to = delegate_to
        self.location = location
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Any):
        hook = ClickHouseHook(clickhouse_conn_id=self.clickhouse_conn_id)
        df = hook.get_pandas_df(self.query)
        bq_hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
            use_legacy_sql=False
        )
        print(df)
        df.to_gbq(
            project_id=self.project_id,
            destination_table=self.target_table,
            table_schema=self.table_schema,
            if_exists='append',
            credentials=bq_hook._get_credentials()
        )
