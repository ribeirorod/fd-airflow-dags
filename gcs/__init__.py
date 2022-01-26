import io
import logging
from abc import ABC

import pandas as pd
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class GCSBaseOperator(BaseOperator, ABC):

    template_fields = ["bucket", "object_name"]
    template_fields_renderers = {"bucket": "py", "object_name": "py"}

    def __init__(self, *, gcs_conn_id: str, bucket: str, object_name: str, **kwargs):
        super().__init__(**kwargs)
        self.gcs_conn_id = gcs_conn_id
        self.bucket = bucket
        self.object_name = object_name

    def upload_csv_df(self, df: pd.DataFrame):
        logging.info("Preparing to upload dataframe with shape: %s to bucket: %s as file: %s",
                     df.shape, self.bucket, self.object_name)
        csv_data = io.StringIO()
        df.to_csv(csv_data, index=False)
        gcs_hook = GCSHook(gcp_conn_id=self.gcs_conn_id)
        gcs_hook.upload(bucket_name=self.bucket,
                             mime_type='text/csv',
                             object_name=self.object_name,
                             data=csv_data.getvalue())
        logging.info("Upload complete")

    def upload_file(self, filename: str):
        logging.info('Uploading local file: %s', filename)
        gcs_hook = GCSHook(gcp_conn_id=self.gcs_conn_id)
        gcs_hook.upload(bucket_name=self.bucket,
                        mime_type='application/json',
                        object_name=self.object_name,
                        filename=filename)
        logging.info("Upload complete")


    def upload_json_df(self, df: pd.DataFrame):
        logging.info("Preparing to upload dataframe with shape: %s to bucket: %s as file: %s",
                     df.shape, self.bucket, self.object_name)
        buffer = io.StringIO()
        df.to_json(buffer, orient="records", lines=True)
        gcs_hook = GCSHook(gcp_conn_id=self.gcs_conn_id)
        gcs_hook.upload(bucket_name=self.bucket,
                        mime_type='application/json',
                        object_name=self.object_name,
                        data=buffer.getvalue())
        logging.info("Upload complete")
