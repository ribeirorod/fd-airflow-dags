import logging
from typing import List

from sqlalchemy import or_, func

from optimizations.models import OptimizationTask
from .abstract import DashOptimizationsOperator


class BlockPublishersOffersOptimizationsOperator(DashOptimizationsOperator):
    def get_actions(self) -> List[OptimizationTask]:
        return self.session.query(OptimizationTask).filter(

            OptimizationTask.change_type == 'block_tsp',
            OptimizationTask.status == 'registered',
            or_(
                OptimizationTask.delay_hours == 0,
                OptimizationTask.email_sent_on + func.make_interval(0, 0, 0, 0, OptimizationTask.delay_hours) < func.now()
            )
        ).all()

    def execute_action(self, action: OptimizationTask):
        logging.info("Executing blocking: %s", action)
        action.status = 'success'
        self.create_event_log('blocked_tsp', action)
        if self.is_dry_run:
            logging.info('Dry run enabled, skipping...')
        else:
            self.dash_client.block_publisher(
                publisher_id=action.publisher_id,
                affpub_ids=[],
                offer_ids=action.offer_id
            )
