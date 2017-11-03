from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DATETIME, PrimaryKeyConstraint
from sqlalchemy.dialects.mysql import SMALLINT, TINYINT


Base = declarative_base()


class AsinRelation(Base):
    __tablename__ = 'asin_relation'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='PK_id'),
    )
    id = Column(Integer)
    platform = Column(String(32), nullable=False, default='')
    asin = Column(String(16), nullable=False, default='')
    type = Column(TINYINT, nullable=False, default=0)
    rasin = Column(String(16), nullable=False, default='')
    rank = Column(SMALLINT, nullable=False, default=0)
    date = Column(DATETIME, nullable=False, default='')
