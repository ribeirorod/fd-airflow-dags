from sqlalchemy import Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base

DashMetadataBase = declarative_base()


class DashOffer(DashMetadataBase):
    __tablename__ = 'dash_offers'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class DashPartner(DashMetadataBase):
    __tablename__ = 'dash_partners'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class DashAdvertiser(DashMetadataBase):
    __tablename__ = 'dash_advertisers'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class DashUser(DashMetadataBase):
    __tablename__ = 'dash_users'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class DashBlockedPartner(DashMetadataBase):
    __tablename__ = 'dash_blocked_partners'
    tracking_link_id = Column(Integer, primary_key=True)
    partner_id = Column(Integer)
    offer_id = Column(Integer)

class DashBlockedLink(DashMetadataBase):
    __tablename__ = 'dash_blocked_link'
    tracking_link_id = Column(Integer, primary_key=True)
    affpubid = Column(JSONB)


