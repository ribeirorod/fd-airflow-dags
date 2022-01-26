import abc
import logging

import pandas as pd
from airflow.providers.http.hooks.http import HttpHook
import requests.auth
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session

from gcs import GCSBaseOperator


class TaboolaAuth(requests.auth.AuthBase):

    def __init__(self, client_id: str, client_secret: str) -> None:
        super().__init__()
        client = BackendApplicationClient(client_id=client_id)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(
            token_url="https://backstage.taboola.com/backstage/oauth/token",
            client_id=client_id,
            client_secret=client_secret
        )
        logging.info("got token: %s", token)
        self.token = token['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f'Bearer {self.token}'
        r.headers['Content-Type'] = 'application/json'
        return r


class TaboolaToGCSOperator(GCSBaseOperator, metaclass=abc.ABCMeta):

    template_fields = ["date_from", "date_to", "bucket", "object_name"]

    def __init__(self, *, date_from: str, date_to: str, connection_id: str, **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to
        self.connection_id = connection_id
        self.hook = None

    @abc.abstractmethod
    def get_data(self) -> pd.DataFrame:
        pass

    def execute(self, context):
        self.hook = HttpHook(http_conn_id=self.connection_id, auth_type=TaboolaAuth, method='GET')
        df = self.get_data()
        logging.info("Created dataframe with shape %s", df.shape)
        logging.info(df)
        self.upload_csv_df(df)


class TaboolaCostToGCSOperator(TaboolaToGCSOperator):

    def get_data(self) -> pd.DataFrame:
        response = self.hook.run(
            endpoint="/backstage/api/1.0/taboolaaccount-bertrampecherfreenetdigitalcom/reports/campaign-summary/dimensions/day",
            data={'start_date': self.date_from, 'end_date': self.date_to}
        )
        df = pd.json_normalize(response.json(), record_path=["results"])
        df["account_id"] = "taboolaaccount-bertrampecherfreenetdigitalcom"
        return df


class TaboolaRevenueToGCSOperator(TaboolaToGCSOperator):
    def get_data(self) -> pd.DataFrame:
        response = self.hook.run(
            endpoint="/backstage/api/1.0/users/current/allowed-accounts",
        )
        publishers = [pub["account_id"] for pub in response.json()["results"]]
        logging.info("Retrieved publishers: %s", publishers)

        results = []
        for pub in publishers:
            response = self.hook.run(
                endpoint=f"/backstage/api/1.0/{pub}/reports/revenue-summary/dimensions/day",
                data={'start_date': self.date_from, 'end_date': self.date_to}
            )
            df = pd.json_normalize(response.json(), record_path=["results"])
            df["account_id"] = pub
            results.append(df)
        df = pd.concat(results, ignore_index=True)
        return df
