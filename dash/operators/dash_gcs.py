from typing import Any

import pandas
from airflow.hooks.base import BaseHook

from dash.client import DashApiClient
from gcs import GCSBaseOperator


class DashToGCSOperator(GCSBaseOperator):

    template_fields = ["bucket", "object_name"]

    def __init__(self, *, dash_connection_id: str, path: str, chunk_size=100, dash_params={}, **kwargs):
        super().__init__(**kwargs)
        self.dash_connection_id = dash_connection_id
        self.path = path
        self.chunk_size = chunk_size
        self.dash_params = dash_params

    def extract_data(self, response: dict):
        return response.get('data', [])

    def get_params(self):
        return self.dash_params

    def execute(self, context: Any):
        connection = BaseHook.get_connection(self.dash_connection_id)
        client = DashApiClient(connection, self.chunk_size)

        response = client.get_list_page(self.path, params=self.get_params())
        total = response.get('meta', {}).get('total', 0)
        offset = self.chunk_size
        data = self.extract_data(response)
        while offset < total:
            response = client.get_list_page(self.path, offset, params=self.get_params())

            # TODO: hack for analytics with dynamic total
            total = response.get('meta', {}).get('total', 0)

            data += self.extract_data(response)
            offset += self.chunk_size

        df = pandas.json_normalize(data, sep="_")
        df.drop_duplicates(inplace=True)

        self.upload_json_df(df)

        return self.object_name


class DashAnalyticsToGCSOperator(DashToGCSOperator):

    template_fields = ["date_from", "date_to", "bucket", "object_name"]
    template_fields_renderers = {"date_from": "py", "date_to": "py", "bucket": "py", "object_name": "py"}

    def __init__(self, *, date_from: str, date_to: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to

    def get_params(self):
        return {**self.dash_params, **{'ts[start]': self.date_from, 'ts[end]': self.date_to}}

    def extract_data(self, response: dict):
        return response.get('data', {'results': []}).get('results', [])
