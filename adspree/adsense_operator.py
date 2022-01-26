from typing import Dict, List

import pandas as pd
from airflow.models import Connection
from googleapiclient.discovery import build
from oauth2client.client import OAuth2Credentials

from gcs import GCSBaseOperator


class AdsenseToGCSOperator(GCSBaseOperator):

    template_fields = ["bucket", "object_name"]

    def __init__(self, *, account_id: str, adsense_connection_id: str,
                 metrics: List[str], dimensions: List[str], date_range: str = "LAST_30_DAYS", **kwargs):
        super().__init__(**kwargs)
        self.adsense_connection_id = adsense_connection_id
        self.account_id = account_id
        self.metrics = metrics
        self.dimensions = dimensions
        self.date_range = date_range

    def execute(self, context: Dict) -> None:
        connection = Connection.get_connection_from_secrets(self.adsense_connection_id)
        service = build("adsense", "v2", credentials=OAuth2Credentials.from_json(connection.extra), cache_discovery=False)

        result = service.accounts().reports().generate(
            account=f"accounts/pub-{self.account_id}",
            dateRange=self.date_range,
            metrics=self.metrics,
            dimensions=self.dimensions,
        ).execute()

        headers = [x['name'].lower() for x in result['headers']]
        processed_rows = []
        for row in result['rows']:
            processed_rows.append(dict(zip(headers, [x['value'] for x in row['cells']])))
        df = pd.DataFrame(processed_rows)
        print(df)

        self.upload_csv_df(df)
