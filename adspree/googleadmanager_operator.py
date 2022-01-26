import datetime
import json
import logging
import tempfile
from typing import Any, List

import googleads
import googleads.common
import pandas as pd
from airflow.models import Connection
from googleads import oauth2, ad_manager
from googleads.oauth2 import GoogleServiceAccountClient

from gcs import GCSBaseOperator
from google.oauth2.service_account import Credentials


class PatchedGoogleServiceAccountClient(GoogleServiceAccountClient):

    def __init__(self, info, scope, sub=None, proxy_config=None):
        self.creds = Credentials.from_service_account_info(info, scopes=[scope], subject=sub)
        self.proxy_config = (proxy_config if proxy_config else googleads.common.ProxyConfig())
        self.Refresh()


class GoogleAdManagerToGCSOperator(GCSBaseOperator):

    application_name = 'dip_airflow'
    template_fields = ["bucket", "object_name", "date_from", "date_to"]

    def __init__(self,
                 *,
                 date_from: str,
                 date_to: str,
                 connection_id: str,
                 dimensions: List[str],
                 columns: List[str],
                 network_code: int,
                 timezone_type: str = 'PUBLISHER',
                 **kwargs):
        super().__init__(**kwargs)
        self.date_from = date_from
        self.date_to = date_to
        self.connection_id = connection_id
        self.dimensions = dimensions
        self.columns = columns
        self.network_code = network_code
        self.timezone_type = timezone_type

    def execute(self, context: Any):

        connection = Connection.get_connection_from_secrets(self.connection_id)
        oauth2_client = PatchedGoogleServiceAccountClient(json.loads(connection.extra_dejson['extra__google_cloud_platform__keyfile_dict']), oauth2.GetAPIScope('ad_manager'))
        ad_manager_client = ad_manager.AdManagerClient(oauth2_client, self.application_name, network_code=self.network_code)

        report_downloader = ad_manager_client.GetDataDownloader(version='v202105')

        start = datetime.datetime.strptime(self.date_from, '%Y-%m-%d')
        end = datetime.datetime.strptime(self.date_to, '%Y-%m-%d')

        report_job = {
            'reportQuery': {
                'dimensions': self.dimensions,
                'columns': self.columns,
                'dateRangeType': 'CUSTOM_DATE',
                'startDate': {'year': start.year, 'month': start.month, 'day': start.day},
                'endDate': {'year': end.year, 'month': end.month, 'day': end.day},
                'timeZoneType': self.timezone_type
            }
        }

        logging.info('Creating report job: %s', report_job)

        report_job_id = report_downloader.WaitForReport(report_job)
        report_file = tempfile.NamedTemporaryFile(suffix='.csv', delete=True)

        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file, use_gzip_compression=False)
        logging.info('File downloaded to: %s', report_file.name)

        report_file.seek(0)
        df = pd.read_csv(report_file)
        df.rename(columns=dict([(column, column.split('.')[1].lower()) for column in df.columns]), inplace=True)
        print(df)
        print(df.dtypes)

        self.upload_csv_df(df)
