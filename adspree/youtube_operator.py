import io
import logging
from time import sleep
from typing import Dict, List

import pandas as pd
import requests
from airflow.models import Connection
from googleapiclient.discovery import build
from oauth2client.client import OAuth2Credentials
from pandas.errors import ParserError

from gcs import GCSBaseOperator


class YoutubeToGCSOperator(GCSBaseOperator):
    template_fields = ["bucket", "object_name", "created_after"]

    def __init__(
            self,
            *,
            youtube_connection_id: str,
            content_owners: List[Dict],
            report_type_id: str,
            created_after: str,
            **kwargs):
        super().__init__(**kwargs)
        self.youtube_connection_id = youtube_connection_id
        self.content_owners = content_owners
        self.report_type_id = report_type_id
        self.created_after = created_after

    def execute(self, context: Dict) -> None:
        connection = Connection.get_connection_from_secrets(self.youtube_connection_id)
        credentials = OAuth2Credentials.from_json(connection.extra)
        service = build("youtubereporting", "v1", credentials=credentials, cache_discovery=False)
        result_df = None
        for content_owner in self.content_owners:
            result = service.jobs().list(
                onBehalfOfContentOwner=content_owner['id']
            ).execute()
            jobs = result['jobs']
            logging.info('Found jobs: %s', jobs)
            revenue_jobs = filter(lambda x: x['reportTypeId'] == self.report_type_id, jobs)
            for revenue_job in revenue_jobs:
                logging.info('Getting reports created after %s for job: %s', context.get('ts'), revenue_job)
                result = service.jobs().reports().list(
                    onBehalfOfContentOwner=content_owner['id'],
                    jobId=revenue_job.get('id'),
                    createdAfter=self.created_after
                ).execute()
                for report in result['reports']:
                    logging.info('Processing: %s', report)
                    data = requests.get(report['downloadUrl'],
                                        headers={'Authorization': f'Bearer {credentials.access_token}'})
                    try:
                        df = pd.read_csv(io.BytesIO(data.content))
                        df['content_owner'] = content_owner['name']
                        df['content_owner_id'] = content_owner['id']
                        df['report_id'] = report['id']

                        if result_df is None:
                            result_df = df
                        else:
                            result_df = pd.concat([result_df, df])
                    except ParserError:
                        logging.error("Could not read data from report [%s]", report)
                        logging.error(data.content)
                    sleep(1)
        self.upload_csv_df(result_df)
