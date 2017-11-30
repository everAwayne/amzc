from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TEXT, TIMESTAMP, PrimaryKeyConstraint
from sqlalchemy.dialects.mysql import SMALLINT, TINYINT, DECIMAL
Base = declarative_base()


class AMZTaskStatistic(Base):
    __tablename__ = 'amz_task_statistic'
    __table_args__ = (
        PrimaryKeyConstraint('id', name='PK_id'),
    )
    id = Column(Integer)
    name = Column(String(64), nullable=False, default='')
    count = Column(Integer, nullable=False, default=0)
    time = Column(TIMESTAMP, nullable=False, default='')
