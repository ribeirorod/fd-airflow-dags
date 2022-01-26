import logging
import shutil
from pathlib import Path
from typing import Any, List, Dict

import numpy as np
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from pandas import json_normalize

import requests
import datetime

from gcs import GCSBaseOperator


class SpiderAfToGcs(GCSBaseOperator):

    def __init__(
            self,
            *,
            spideraf_connection_id: str,
            report_type: str,
            schema_fields: List[Dict],
            chunk_size=500,
            **kwargs):
        super().__init__(**kwargs)
        self.spideraf_connection_id = spideraf_connection_id
        self.report_type = report_type
        self.schema_fields = schema_fields
        self.chunk_size = chunk_size
        self.connection = None

    def _get_page(self, path: str, offset=0, retry_count=0):
        logging.info("Requesting: %s with offset %s", path, offset)

        response = requests.get('https://{}{}'.format(self.connection.host, path),
                                params={"api_token": self.connection.password, "limit": self.chunk_size,
                                        "offset": offset})
        if response.status_code == 404:
            logging.warning("Report not found, skipping...")
            raise AirflowSkipException()
        elif response.status_code != 200:
            if retry_count < 3:
                logging.warning("Spideraf responded with code=%s, body=%s, retrying...", response.status_code,
                                response.json())
                return self._get_page(path, offset, retry_count + 1)
            else:
                logging.error("Spideraf responded with code=%s, body=%s", response.status_code, response.json())
                raise AirflowFailException()
        logging.info("Received: %s", response.status_code)
        return response.json().get("data", [])

    def get_reports(self, report_type: str):
        return self._get_paginated(path="/api/v1/reports/{}".format(report_type))

    def get_sites(self, report_type: str, report_id: int):
        return self._get_paginated(path="/api/v1/reports/{}/{}/sites".format(report_type, report_id))

    def _get_paginated(self, path: str):
        data = self._get_page(path)
        if len(data) == self.chunk_size:
            offset = self.chunk_size
            while True:
                page = self._get_page(path, offset=offset)
                data += page
                offset += self.chunk_size
                if len(page) < self.chunk_size:
                    break
        return data

    def execute(self, context: Any):
        execution_date = context.get('execution_date')
        logging.info('Execution date is: %s', execution_date)
        self.connection = BaseHook.get_connection(self.spideraf_connection_id)
        reports = self.get_reports(self.report_type)
        sites = []
        for report in reports:
            report_updated = datetime.datetime.strptime(report.get('report_updated_at'), '%Y-%m-%d %H:%M:%S %z')
            if report_updated < execution_date - datetime.timedelta(days=3):
                logging.info('Report %s last updated on %s, skipping...', report.get('id'), report_updated)
            else:
                sites += self.get_sites(self.report_type, report['id'])

        df = json_normalize(sites, sep='_')
        for field in self.schema_fields:
            if field['name'] not in df.columns:
                logging.info('df is missing field [%s]', field)
                if field['type'] in ['INTEGER', 'FLOAT']:
                    df[field['name']] = np.NaN
                else:
                    df[field['name']] = None
        self.upload_csv_df(df)

        return self.object_name


class GCSUploadOperator(BaseOperator):
    template_fields = ["source_obj", "target_obj"]

    def __init__(self, *, source_gcp_connection: str, target_gcp_connection: str, source_obj: str, target_obj: str,
                 source_bucket: str, target_bucket: str, **kwargs):
        super().__init__(**kwargs)
        self.source_gcp_connection = source_gcp_connection
        self.target_gcp_connection = target_gcp_connection
        self.source_obj = source_obj
        self.target_obj = target_obj
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket

    def execute(self, context: Any):
        source_hook = GCSHook(gcp_conn_id=self.source_gcp_connection)
        target_hook = GCSHook(gcp_conn_id=self.target_gcp_connection)

        Path("temp").mkdir(exist_ok=True)
        logging.info('Downloading file: %s/%s', self.source_bucket, self.source_obj)
        source_hook.download(object_name=self.source_obj, bucket_name=self.source_bucket, filename='temp/download.csv')
        logging.info('Uploading to: %s/%s', self.target_bucket, self.target_obj)
        target_hook.upload(object_name=self.target_obj, bucket_name=self.target_bucket, filename='temp/download.csv')
        shutil.rmtree('temp/')
