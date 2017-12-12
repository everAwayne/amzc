import json
import zlib
import time
import asyncio
import aiohttp
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_
from datetime import datetime
import pipeflow
from .models import AMZTaskStatistic
from util.log import logger
from util.task_protocal import TaskProtocal
from pipeflow import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from config import RABBITMQ_CONF, STATS_DB_USER_NAME, STATS_DB_USER_PW, STATS_DB_SEVER_ADDR


MAX_WORKERS = 1
FLUSH_INTERVAL = 60
TID_MAP = {
    ('1', 0): 'product_task',
    ('2', 0): 'review_task',
    ('3', 0): 'qa_task',
    ('4', 0): 'keyword_task',
}
last_flush_time = time.time()
data_dct = {}


DB_USER_NAME = STATS_DB_USER_NAME
DB_USER_PW = STATS_DB_USER_PW
DB_SEVER_ADDR = STATS_DB_SEVER_ADDR
DB_DATABASE_NAME = "statistics"
DB_CHARSET = "utf8"
SQLALCHEMY_ECHO = False  #回显执行的SQL(DEBUG用)
SQLALCHEMY_POOL_SIZE = 0   #连接池大小，0意味无限制
SQLALCHEMY_POOL_MAX_OVERFLOW = -1    #连接池额外可增加上限
SQLALCHEMY_POOL_RECYCLE = 120    #连接池回收连接时间
SQLALCHEMY_DATABASE_URI = \
    "mysql+pymysql://{name:s}:{pw:s}@{addr:s}/{db:s}?charset={charset:s}"\
    .format(
    name=DB_USER_NAME,
    pw=DB_USER_PW,
    addr=DB_SEVER_ADDR,
    db=DB_DATABASE_NAME,
    charset=DB_CHARSET,
)
engine = create_engine(SQLALCHEMY_DATABASE_URI,
    #convert_unicode=True,
    echo=SQLALCHEMY_ECHO,
    pool_size=SQLALCHEMY_POOL_SIZE,
    max_overflow=SQLALCHEMY_POOL_MAX_OVERFLOW,
    pool_recycle=SQLALCHEMY_POOL_RECYCLE,
)
db_session_mk = sessionmaker(bind=engine)


def flush_data():
    global last_flush_time
    db_session = db_session_mk()
    time_now = datetime.now().strftime("%Y-%m-%d 00:00:00")
    add_ls = []
    for name in data_dct:
        record = db_session.query(AMZTaskStatistic)\
                .filter(and_(AMZTaskStatistic.name==name, AMZTaskStatistic.time==time_now))\
                .first()
        if not record:
            amz_stats = AMZTaskStatistic()
            amz_stats.name = name
            amz_stats.count = data_dct[name]
            amz_stats.time = time_now
            add_ls.append(amz_stats)
        else:
            record.count += data_dct[name]
    if add_ls:
        db_session.bulk_save_objects(add_ls)
    try:
        db_session.commit()
    except:
        logger.error("flush db error")
        db_session.rollback()
    else:
        last_flush_time = time.time()
    finally:
        data_dct.clear()
        db_session.close()


async def auto_flush(server):
    while True:
        time_now = time.time()
        if data_dct and time_now > last_flush_time + FLUSH_INTERVAL:
            flush_data()
        await asyncio.sleep(FLUSH_INTERVAL)


async def handle_worker(group, task):
    """Handle statistic task
    """
    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    if 'extra' in task_dct and 'stats' in task_dct['extra']:
        tid = task_dct['extra']['stats'].get('tid')
        step = task_dct['extra']['stats'].get('step')
        if (tid, step) in TID_MAP:
            stats_name = TID_MAP[(tid, step)]
            data_dct.setdefault(stats_name, 0)
            data_dct[stats_name] += 1


def run():
    input_end = RabbitmqInputEndpoint('statistic:input', **RABBITMQ_CONF)
    server = pipeflow.Server()
    server.add_worker(auto_flush)
    group = server.add_group('main', MAX_WORKERS)
    group.add_input_endpoint('input', input_end)
    group.set_handle(handle_worker)
    server.run()
