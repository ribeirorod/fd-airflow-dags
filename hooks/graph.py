from functools import cached_property
from typing import List

import msal
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from office365.graph_client import GraphClient


class MicrosoftGraphHook(BaseHook):

    def __init__(self, conn_id: str = 'microsoft_graph_default') -> None:
        super().__init__()
        self.conn_id = conn_id

    def _authorize(self):
        connection = Connection.get_connection_from_secrets(self.conn_id)

        authority_url = f"https://login.microsoftonline.com/{connection.extra_dejson['tenant_id']}"
        app = msal.ConfidentialClientApplication(
            authority=authority_url,
            client_id=connection.extra_dejson['client_id'],
            client_credential=connection.extra_dejson['client_secret']
        )

        return app.acquire_token_by_refresh_token(
            connection.extra_dejson['refresh_token'],
            scopes=['https://graph.microsoft.com/.default'])

    @cached_property
    def _get_client(self):
        client = GraphClient(self._authorize)
        return client

    def send_email(
            self,
            subject,
            to: List[str],
            content: str,
            content_type='Html',
            cc: List[str] = None,
            bcc: List[str] = None,
            from_address: str = 'no-reply@applift.com'
    ):
        if bcc is None:
            bcc = []
        if cc is None:
            cc = []
        client = self._get_client
        message_json = {
            "Message": {
                "Subject": subject,
                "Body": {
                    "ContentType": content_type,
                    "Content": content
                },
                "ToRecipients": [{'EmailAddress': {'Address': email}} for email in to],
                "CcRecipients": [{'EmailAddress': {'Address': email}} for email in cc],
                "BccRecipients": [{'EmailAddress': {'Address': email}} for email in bcc],
                "From": {'EmailAddress': {'Address': from_address}},
            },
            "SaveToSentItems": "true"
        }
        client.me.send_mail(message_json).execute_query()
