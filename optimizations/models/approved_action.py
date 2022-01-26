from sqlalchemy import Column, Integer, String, JSON, Boolean, TIMESTAMP, func

from optimizations.models import OptimizationsMetadataBase


class ApprovedAction(OptimizationsMetadataBase):
    __tablename__ = 'ym_approved_actions'
    id = Column(Integer, primary_key=True)
    suggested_id = Column(Integer)
    advertiser_id = Column(Integer)
    publisher_id = Column(Integer)
    offer_id = Column(Integer)
    tsp = Column(Integer)
    offer_name = Column(String)
    publisher_name = Column(String)
    offer_page = Column(String)
    source = Column(String)
    publisher_account_manager_name = Column(String)
    reasons = Column(String)
    change_type = Column(Integer)
    delay_hours = Column(Integer)
    process_status = Column(Integer)
    is_cancelled = Column(Integer)
    email_to = Column(String)
    email_cc = Column(String)
    email_bcc = Column(String)
    updated_by = Column(String)
    template_id = Column(Integer)
    action_details = Column(JSON)
    goal_id = Column(Integer)
    is_test = Column(Boolean)
    created_on = Column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_on = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.current_timestamp())