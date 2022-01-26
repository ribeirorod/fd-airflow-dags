import abc
import datetime
import logging
from abc import ABC
from functools import cached_property
from typing import Any, List

from airflow.hooks.base import BaseHook
from airflow.models import BaseOperator, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import sessionmaker

from optimizations.models import EventLog, OptimizationTask
from dash.client import DashApiClient


class OptimizationsOperator(BaseOperator, metaclass=abc.ABCMeta):
    def __init__(self, *, db_connection_id: str, **kwargs):
        super().__init__(**kwargs)
        self.db_connection_id = db_connection_id

    def create_event_log(self, event_type, action: OptimizationTask, message=None):
        event_log = EventLog(
            event_type=event_type,
            task_id=action.id,
            message=message,
            created_on=datetime.datetime.utcnow()
        )
        self.session.add(event_log)

    @cached_property
    def is_dry_run(self):
        return Variable.get('optimization-dry-run', default_var=True, deserialize_json=True)

    @cached_property
    def db_hook(self):
        return PostgresHook.get_hook(conn_id=self.db_connection_id)

    @cached_property
    def session(self):
        orm_sessionmaker = sessionmaker(bind=self.db_hook.get_sqlalchemy_engine())
        return orm_sessionmaker()

    def execute(self, context: Any):
        if self.is_dry_run:
            logging.info('Running the operator as dry run...')
        logging.info("Getting actions")
        actions = self.get_actions()
        logging.info("Found %s actions to process", len(actions))
        for action in actions:
            logging.info("Processing action: %s", action)
            self.execute_action(action)
        self.session.commit()

    @abc.abstractmethod
    def get_actions(self) -> List[OptimizationTask]:
        pass

    @abc.abstractmethod
    def execute_action(self, action: OptimizationTask):
        pass


class DashOptimizationsOperator(OptimizationsOperator, ABC):

    def __init__(self, *, dash_connection_id: str, **kwargs):
        super().__init__(**kwargs)
        self.dash_connection_id = dash_connection_id

    @cached_property
    def dash_client(self):
        return DashApiClient(connection=BaseHook.get_connection(self.dash_connection_id))
