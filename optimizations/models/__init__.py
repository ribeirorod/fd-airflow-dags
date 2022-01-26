from sqlalchemy import Column, Integer, String, TIMESTAMP, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.ext.declarative import declarative_base

OptimizationsMetadataBase = declarative_base()

from .approved_action import ApprovedAction
from .event_log import EventLog


class OptimizationTask(OptimizationsMetadataBase):
    __tablename__ = 'optimizations_tasks'
    __table_args__ = {'schema': 'optimizations'}
    id = Column(Integer, primary_key=True)
    advertiser_id = Column(Integer)
    publisher_id = Column(Integer)
    offer_id = Column(Integer)
    tsp = Column(Integer)
    offer_name = Column(String)
    publisher_name = Column(String)
    affpubid = Column(String)
    publisher_account_manager_name = Column(String)
    reason = Column(String)
    change_type = Column(String)
    delay_hours = Column(Integer)
    status = Column(String)
    email_to = Column(ARRAY(String))
    email_cc = Column(ARRAY(String))
    email_bcc = Column(ARRAY(String))
    updated_by = Column(String)
    created_on = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_on = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.current_timestamp())
    email_sent_on = Column(TIMESTAMP(timezone=True))
