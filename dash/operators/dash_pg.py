import abc
from typing import Any
import logging

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from ..models import DashBlockedLink, DashBlockedPartner, DashUser, DashAdvertiser, DashPartner, DashOffer
from ..client import DashApiClient
from sqlalchemy.orm import sessionmaker

# TODO: deprecated
class DashToPgOperator(BaseOperator, metaclass=abc.ABCMeta):
    def __init__(self, *, postgres_connection_id: str, dash_connection_id: str, path: str, chunk_size=100, **kwargs):
        super().__init__(**kwargs)
        self.connection = BaseHook.get_connection(dash_connection_id)
        self.path = path
        self.client = DashApiClient(self.connection, chunk_size)
        self.chunk_size = chunk_size
        metadata_hook = PostgresHook.get_hook(conn_id=postgres_connection_id)
        self.orm_sessionmaker = sessionmaker(bind=metadata_hook.get_sqlalchemy_engine())

    def execute(self, context: Any):
        response = self.client.get_list_page(self.path)
        total = response.get('meta', {}).get('total', 0)
        offset = self.chunk_size
        self.process_page(response.get('data', []))

        while offset < total:
            response = self.client.get_list_page(self.path, offset)
            self.process_page(response.get('data', []))
            offset += self.chunk_size
        return total

    def process_page(self, data: list):
        logging.info("Persisting to db")
        session = self.orm_sessionmaker()
        [session.merge(self.transform_item(item)) for item in data]
        session.commit()
        logging.info("Session committed")
        return data

    @abc.abstractmethod
    def transform_item(self, item):
        raise NotImplemented


class DashOffersToPgOperator(DashToPgOperator):

    def __init__(self, *, dash_connection_id: str, chunk_size=100, **kwargs):
        super().__init__(dash_connection_id=dash_connection_id,
                         path='/api/dash/v1/offers',
                         auth_method='bearer',
                         chunk_size=chunk_size, **kwargs)

    def transform_item(self, item):
        return DashOffer(id=item['id'], name=item['name'])


class DashPartnersToPgOperator(DashToPgOperator):

    def __init__(self, *, dash_connection_id: str, chunk_size=100, **kwargs):
        super().__init__(dash_connection_id=dash_connection_id,
                         path='/api/dash/v1/partners',
                         auth_method='bearer',
                         chunk_size=chunk_size, **kwargs)

    def transform_item(self, item):
        return DashPartner(id=item['id'], name=item['short'])


class DashAdvertisersToPgOperator(DashToPgOperator):

    def __init__(self, *, dash_connection_id: str, chunk_size=100, **kwargs):
        super().__init__(dash_connection_id=dash_connection_id,
                         path='/api/dash/v1/advertisers',
                         auth_method='bearer',
                         chunk_size=chunk_size, **kwargs)

    def transform_item(self, item):
        return DashAdvertiser(id=item['id'], name=item['short'])


class DashUsersToPgOperator(DashToPgOperator):

    def __init__(self, *, dash_connection_id: str, chunk_size=100, **kwargs):
        super().__init__(dash_connection_id=dash_connection_id,
                         path='/api/dash/v1/users',
                         auth_method='bearer',
                         chunk_size=chunk_size, **kwargs)

    def transform_item(self, item):
        return DashUser(id=item['id'], name=item['email'])


class DashBlockedLinksToPgOperator(DashToPgOperator):

    def __init__(self, *, dash_connection_id: str, chunk_size=100, **kwargs):
        super().__init__(dash_connection_id=dash_connection_id,
                         path='/api/internal/v1/blocking/tsp',
                         auth_method='x-auth',
                         chunk_size=chunk_size, **kwargs)

    def transform_item(self, item):
        return DashBlockedLink(tracking_link_id=item['tracking_link_id'], affpubid=item['affpubid'])


class DashBlockedPartnersToPgOperator(DashToPgOperator):

    def __init__(self, *, dash_connection_id: str, chunk_size=100, **kwargs):
        super().__init__(dash_connection_id=dash_connection_id,
                         path='/api/internal/v1/blocking/partners',
                         auth_method='x-auth',
                         chunk_size=chunk_size, **kwargs)

    def transform_item(self, item):
        return DashBlockedPartner(tracking_link_id=item['tracking_link_id'],
                                  offer_id=item['offer_id'],
                                  partner_id=item['partner_id']
                                  )

