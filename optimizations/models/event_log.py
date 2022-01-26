from sqlalchemy import Column, Integer, String, TIMESTAMP, func

from optimizations.models import OptimizationsMetadataBase


class EventLog(OptimizationsMetadataBase):
    __tablename__ = 'optimizations_log'
    __table_args__ = {'schema': 'optimizations'}
    id = Column(Integer, primary_key=True)
    event_type = Column(String)
    message = Column(String)
    task_id = Column(Integer)
    created_on = Column(TIMESTAMP(timezone=True), server_default=func.now())