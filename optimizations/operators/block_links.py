import logging
from operator import or_
from typing import List

from sqlalchemy import func

from optimizations.models import OptimizationTask
from .abstract import DashOptimizationsOperator


class BlockLinksOptimizationsOperator(DashOptimizationsOperator):
    def get_actions(self) -> List[OptimizationTask]:
        return self.session.query(OptimizationTask).filter(

            OptimizationTask.change_type == 'block_affpubid',
            OptimizationTask.status == 'registered',
            or_(
                OptimizationTask.delay_hours == 0,
                OptimizationTask.email_sent_on + func.make_interval(0, 0, 0, 0, OptimizationTask.delay_hours) < func.now()
            )
        ).all()

    def execute_action(self, action: OptimizationTask):
        logging.info("Executing link: %s", action)
        action.status = 'success'
        self.create_event_log('blocked_affpubid', action)
        if self.is_dry_run:
            logging.info('Dry run enabled, skipping...')
        else:
            self.dash_client.block_tsp(
                tsp=action.tsp,
                affpub_ids=[action.affpubid]
            )