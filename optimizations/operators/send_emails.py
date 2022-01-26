import datetime
import logging
import os
from typing import List, Optional, Any

import pytz

from hooks.graph import MicrosoftGraphHook
from optimizations.models import OptimizationTask
from .abstract import OptimizationsOperator
from jinja2 import FileSystemLoader, Environment


class PublisherEmail:
    def __init__(
            self,
            task: OptimizationTask
    ):
        self.rows = []
        self.task = task
        self.tasks = []

    @property
    def deadline(self):
        return (datetime.datetime.now(tz=pytz.timezone('Europe/Berlin')) + datetime.timedelta(
            hours=self.task.delay_hours)).strftime("%b %-d, %Y %-I:%M %p %Z")

    @property
    def to(self):
        return self.task.email_to

    @property
    def cc(self):
        return self.task.email_cc

    @property
    def bcc(self):
        return self.task.email_bcc

    @property
    def change_type(self):
        return self.task.change_type


class SendEmailsOptimizationsOperator(OptimizationsOperator):
    template_mapping = {
        'block_tsp': 'block_offer_template.html',
        'block_affpubid': 'block_link_template.html'
    }

    dry_run_emails = ['konrad.tysiac@mediaelements.com']

    def __init__(self, *, db_connection_id: str, **kwargs):
        super().__init__(db_connection_id=db_connection_id, **kwargs)
        file_loader = FileSystemLoader(f'{os.path.abspath(os.path.dirname(__file__))}/../templates/')
        self.env = Environment(loader=file_loader)

    def get_actions(self) -> List[OptimizationTask]:
        return self.session.query(OptimizationTask).filter(
            OptimizationTask.status == 'registered',
            OptimizationTask.email_sent_on.is_(None),
            OptimizationTask.delay_hours > 0
        ).all()

    def execute(self, context: Any):
        if self.is_dry_run:
            logging.info('Running the operator as dry run...')
        logging.info("Getting actions")
        actions = self.get_actions()
        logging.info("Found %s actions to process", len(actions))
        email_map = {}
        for action in actions:
            if action.change_type not in email_map:
                email_map[action.change_type] = {}
            if action.publisher_id not in email_map[action.change_type]:
                email_map[action.change_type][action.publisher_id] = PublisherEmail(action)

            email_map[action.change_type][action.publisher_id].tasks.append(action)
            email_map[action.change_type][action.publisher_id].rows.append({
                'reason': action.reason,
                'affpubid': action.affpubid,
                'offer_name': action.offer_name,
                'offer_id': action.offer_id
            })
            logging.info("Processing action: %s", action)

        for change_type, emails in email_map.items():
            for publisher_id, email in emails.items():
                logging.info('Sending email for publisher [%s]', publisher_id)
                self.send_email(email)
        self.session.commit()

    def render_email(self, email: PublisherEmail) -> Optional[str]:
        template_name = self.template_mapping.get(email.change_type)
        if template_name is None:
            logging.info('No template mapped for change_type=[%s], skipping sending email...', email.change_type)
            return None
        else:
            template = self.env.get_template(template_name)
            return template.render(rows=email.rows, deadline=email.deadline)

    def execute_action(self, action: OptimizationTask):
        pass

    def send_email(self, email: PublisherEmail):
        logging.info('Sending email to > %s, %s, %s', email.to, email.cc, email.bcc)
        content = self.render_email(email)
        for task in email.tasks:
            task.email_sent_on = datetime.datetime.utcnow()
            self.create_event_log('email_sent', task)
        graph_hook = MicrosoftGraphHook()
        if self.is_dry_run:
            logging.info('Dry run enabled, sending to developer email...')
            graph_hook.send_email(
                subject="PRICEMEISTER TEST",
                to=self.dry_run_emails,
                content=content)
        else:
            graph_hook.send_email(
                subject="Applift Updates",
                to=email.to,
                cc=email.cc,
                bcc=email.bcc,
                content=content)
