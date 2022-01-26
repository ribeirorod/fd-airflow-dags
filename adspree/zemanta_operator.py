import logging
from typing import Dict

import requests
import pandas as pd
import time

from airflow.providers.http.hooks.http import HttpHook
import requests.auth

from gcs import GCSBaseOperator


class ZemantaAuth(requests.auth.AuthBase):
    token = None

    def __init__(self, client_id: str, client_secret: str) -> None:
        super().__init__()
        if ZemantaAuth.token is None:
            response = requests.post("https://oneapi.zemanta.com/o/token/",
                                     data={"grant_type": "client_credentials"},
                                     auth=(client_id, client_secret))
            token = response.json()
            logging.info("got token: %s", token)
            ZemantaAuth.token = token['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        r.headers['Content-Type'] = 'application/json'
        return r


class ZemantaToGCSOperator(GCSBaseOperator):
    template_fields = ["date_from", "date_to", "bucket", "object_name"]
    template_fields_renderers = {
        "date_from": "py",
        "date_to": "py",
        "bucket": "py",
        "object_name": "py",
    }

    def __init__(self, connection_id: str, date_from: str, date_to: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to
        self.hook_post = None
        self.hook_get = None
        self.connection_id = connection_id

    def create_report_job(self):
        response = self.hook_post.run(endpoint="/rest/v1/reports/",
                                      json={
                                          "fields": [
                                              {"field": "Day"},
                                              {"field": "Account Id"},
                                              {"field": "Account"},
                                              {"field": "Campaign Id"},
                                              {"field": "Campaign"},
                                              {"field": "Ad Group Id"},
                                              {"field": "Ad Group"},
                                              {"field": "Content Ad Id"},
                                              {"field": "Content Ad"},
                                              {"field": "Impressions"},
                                              {"field": "Clicks"},
                                              {"field": "CTR"},
                                              {"field": "Avg. CPC"},
                                              {"field": "Avg. CPM"},
                                              {"field": "Media Spend"},
                                              {"field": "License Fee"},
                                              {"field": "Total Spend"},
                                              {"field": "Margin"},
                                              {"field": "Visits"},
                                              {"field": "Unique Users"},
                                              {"field": "New Users"},
                                              {"field": "Returning Users"},
                                              {"field": "% New Users"},
                                              {"field": "Pageviews"},
                                              {"field": "Pageviews per Visit"},
                                              {"field": "Bounced Visits"},
                                              {"field": "Non-Bounced Visits"},
                                              {"field": "Bounce Rate"},
                                          ],
                                          "filters": [
                                              {
                                                  "field": "Date",
                                                  "operator": "between",
                                                  "from": self.date_from,
                                                  "to": self.date_to,
                                              }
                                          ]
                                      })
        data = response.json()
        logging.info("received: %s", data)
        return data["data"]["id"]

    def get_job_status(self, job_id) -> Dict:
        response = self.hook_get.run(endpoint=f"/rest/v1/reports/{job_id}")
        job = response.json().get("data")
        logging.info("Job information: %s", job)
        return job

    def process_data_frame(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(columns={
            "Day": "day",
            "Account Id": "account_id",
            "Account": "account",
            "Campaign Id": "campaign_id",
            "Campaign": "campaign",
            "Ad Group Id": "ad_group_id",
            "Ad Group": "ad_group",
            "Content Ad Id": "content_ad_id",
            "Content Ad": "content_ad",
            "Impressions": "impressions",
            "Clicks": "clicks",
            "CTR": "ctr",
            "Avg. CPC": "avg_cpc",
            "Avg. CPM": "avg_cpm",
            "Media Spend": "media_spend",
            "License Fee": "license_fee",
            "Total Spend": "total_spend",
            "Margin": "margin",
            "Visits": "visits",
            "Unique Users": "unique_users",
            "New Users": "new_users",
            "Returning Users": "returning_users",
            "% New Users": "new_users_pct",
            "Pageviews": "pageviews",
            "Pageviews per Visit": "pageviews_per_visit",
            "Bounced Visits": "bounced_visits",
            "Non-Bounced Visits": "non_bounced_visits",
            "Bounce Rate": "bounce_rate"
        })

    def execute(self, context) -> None:
        self.hook_post = HttpHook(http_conn_id=self.connection_id, auth_type=ZemantaAuth, method='POST')
        self.hook_get = HttpHook(http_conn_id=self.connection_id, auth_type=ZemantaAuth, method='GET')

        job_id = self.create_report_job()
        job_status = self.get_job_status(job_id)

        # TODO: use a sensor
        while job_status["status"] != "DONE":
            job_status = self.get_job_status(job_id)
            logging.info("Current status: %s", job_status["status"])
            time.sleep(20)
        df = pd.read_csv(job_status["result"])
        df = self.process_data_frame(df)
        print(df)
        self.upload_csv_df(df)