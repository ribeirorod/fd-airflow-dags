import logging
import time
from typing import List, Dict

import pandas as pd
from airflow.providers.google.common.hooks.discovery_api import GoogleDiscoveryApiHook

from gcs import GCSBaseOperator


class GoogleAnalyticsToGCSOperator(GCSBaseOperator):
    template_fields = ["date_from", "date_to", "bucket", "object_name"]
    template_fields_renderers = {"date_from": "py", "date_to": "py", "bucket": "py", "object_name": "py"}

    def __init__(self, *, analytics_connection_id: str, date_from: str, date_to: str,
                 metrics: List[Dict], dimensions: List[Dict], **kwargs):
        super().__init__(**kwargs)
        self.analytics_connection_id = analytics_connection_id
        self.date_from = date_from
        self.date_to = date_to
        self.metrics = metrics
        self.dimensions = dimensions

    def build_dataframe(self, response, view_id):
        converted_rows = []
        for report in response.get("reports", []):
            dimensions = report.get("columnHeader", {}).get("dimensions", [])
            metrics = report.get("columnHeader", {}).get("metricHeader", {}).get("metricHeaderEntries", [])
            for row in report.get("data", {}).get("rows", []):
                converted_row = {'view_id': view_id}
                for column, value in zip(dimensions, row["dimensions"]):
                    converted_row[column] = value
                for schema, value in zip(metrics, row.get("metrics", [{}])[0].get("values", [])):
                    if schema["type"] == "INTEGER":
                        converted_row[schema["name"]] = int(value)
                    elif schema["type"] == "FLOAT":
                        converted_row[schema["name"]] = float(value)
                converted_rows.append(converted_row)
        return pd.DataFrame(converted_rows)

    def execute(self, context: Dict) -> None:
        service = GoogleDiscoveryApiHook(
            gcp_conn_id=self.analytics_connection_id,
            api_service_name="analytics",
            api_version="v3").get_conn()
        reporting_service = GoogleDiscoveryApiHook(
            gcp_conn_id=self.analytics_connection_id,
            api_service_name="analyticsreporting",
            api_version="v4").get_conn()

        accounts = service.management().accounts().list().execute()

        view_ids = []

        for account in accounts["items"]:
            logging.info("Pulling properties for account: %s", account)
            properties = service.management().webproperties().list(accountId=account["id"]).execute()
            time.sleep(1)

            for prop in properties["items"]:
                logging.info("Pulling views for account/property: %s/%s", account, prop)
                views = service.management().profiles().list(
                    accountId=account["id"],
                    webPropertyId=prop["id"]).execute()
                time.sleep(1)

                for view in views["items"]:
                    view_ids.append(view["id"])

        appended_data = []
        for view_id in view_ids:
            logging.info("Pulling report for view: %s", view_id)
            response = reporting_service.reports().batchGet(
                body={
                    "reportRequests": [
                        {
                            "viewId": view_id,
                            "dateRanges": [{"startDate": self.date_from, "endDate": self.date_to}],
                            "metrics": self.metrics,
                            "dimensions": self.dimensions,
                        }
                    ]
                }
            ).execute()
            time.sleep(1)

            appended_data.append(self.build_dataframe(response, view_id))

        df = pd.concat(appended_data)
        df.rename(
            columns=dict([(column, column.replace('ga:', '')) for column in df.columns]),
            inplace=True,
        )
        print(df)
        self.upload_csv_df(df)
