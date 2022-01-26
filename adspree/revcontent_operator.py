import logging

import pandas as pd
from airflow.providers.http.hooks.http import HttpHook
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
import requests.auth

from gcs import GCSBaseOperator


class TokenAuth(requests.auth.AuthBase):

    def __init__(self, client_id: str, client_secret: str) -> None:
        super().__init__()
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(
            token_url="https://api.revcontent.io/oauth/token",
            client_id=client_id,
            client_secret=client_secret
        )
        logging.info("got token: %s", token)
        self.token = token['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        r.headers['Content-Type'] = 'application/json'
        return r


class RevcontentToGCSOperator(GCSBaseOperator):

    template_fields = ["date_from", "date_to", "bucket", "object_name"]
    template_fields_renderers = {
        "date_from": "py",
        "date_to": "py",
        "bucket": "py",
        "object_name": "py",
    }

    def __init__(self, *, date_from: str, date_to: str, connection_id: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to
        self.connection_id = connection_id
        self.hook = None

    def list_boosts_target_url(self):
        response = self.hook.run("/stats/api/v1.0/boosts/content", data={
            "date_from": self.date_from,
            "date_to": self.date_to,
        })
        api_response = response.json()
        urls = []
        for item in api_response["data"]:
            urls.append(tuple([item["boost_id"], item["target_url"]]))
        return pd.DataFrame(urls, columns=["boost_id", "target_url"])

    def boost_daily_performance(self):
        limit = 500
        offset = 0
        data = []
        while True:
            logging.info("Fetching performance with offset %s", offset)
            response = self.hook.run("/stats/api/v1.0/boosts/performance", data={
                "date_from": self.date_from,
                "date_to": self.date_to,
                "limit": limit,
                "all_boosts": "yes",
                "offset": offset
            })
            api_response = response.json()
            data = data + api_response["data"]

            if len(api_response["data"]) != limit:
                logging.info("Api returned %s rows, exiting...", len(api_response["data"]))
                break
            offset += limit
        return pd.json_normalize(data)

    def execute(self, context):
        self.hook = HttpHook(http_conn_id=self.connection_id, auth_type=TokenAuth, method='GET')

        urls_df = self.list_boosts_target_url()
        boosts_df = self.boost_daily_performance()
        df = boosts_df.merge(urls_df, how="left", left_on="id", right_on="boost_id")
        df = df.drop("boost_id", axis=1)
        df = df.drop_duplicates()
        logging.info("Successfully created dataframe")
        self.upload_csv_df(df)
        logging.info("Successfully uploaded csv to GCS")
