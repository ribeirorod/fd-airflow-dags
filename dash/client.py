import logging
from typing import List

import requests
from airflow.exceptions import AirflowFailException
from airflow.models import Connection


class DashApiClient:
    def __init__(self, connection: Connection, chunk_size=100):
        self.connection = connection
        self.headers = {
            'X-Authorization': self.connection.extra_dejson['internal_token'],
            'Authorization': self.connection.extra_dejson['dash_token']
        }
        self.chunk_size = chunk_size

    def _get_request(self, path: str, params: dict, log_body=False, retry_count=0):
        logging.info('Requesting method=GET, path=%s, params=%s', path, str(params))
        response = requests.get('https://{}{}'.format(self.connection.host, path),
                                params=params, headers=self.headers)
        logging.info('Received status_code=%s, body=%s',
                     response.status_code,
                     response.json() if log_body else 'TRUNCATED'
        )
        if response.status_code != 200:
            if retry_count < 3:
                logging.warning("Received an error: [%s], retrying...", response.json())
                return self._get_request(path, params, log_body, retry_count + 1)
            else:
                logging.error("Received an error: [%s], exiting...", response.json())
                raise AirflowFailException()
        return response.json()

    def _post_request(self, path: str, payload: dict):
        logging.info('Requesting method=POST, path=%s, payload=%s', path, payload)
        response = requests.post('https://{}{}'.format(self.connection.host, path),
                                 json=payload, headers=self.headers)
        logging.info('Received status_code=%s, body=%s', response.status_code, response.json())
        return response.json()

    def get_list_page(self, path: str, offset=0, params={}):
        pagination = {'limit': self.chunk_size, 'offset': offset}
        return self._get_request(path, {**params, **pagination})

    def block_tsp(self, tsp: int, affpub_ids: List[str]):
        logging.info("Blocking affpubids=%s for tsp=%s", affpub_ids, tsp)
        return self._post_request(f'/api/internal/v1/blocking/tsp/{tsp}', {'affpubids': affpub_ids})

    def block_publisher(self, publisher_id: int, affpub_ids: List[str], offer_ids: List[int]):
        logging.info("Blocking affpubids=%s, offers=%s for publisher=%s", affpub_ids, offer_ids, publisher_id)
        return self._post_request(f'/api/internal/v1/blocking/partners/{publisher_id}',
                                  {'affpubids': affpub_ids, 'offer_ids': offer_ids})
