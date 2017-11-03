import json
import time
import math
import redis
import socket
import asyncio
import functools
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import AsinRelation
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal
from util.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint


POPT_REDIS_CONF = {'host': '192.168.0.10', 'port': 6379, 'db': 10, 'password': None}
POPT_KEY_PREFIX = "bestseller:popt:"
CURVE_FUNC = lambda x,a,b:math.log(a/x+1)/b
popt_map = {}


BATCH_SIZE = 2000
FLUSH_INTERVAL = 60
DELETE_INTERVAL = 86400 * 1
EXPIRE_DAYS = 3
last_flush_time = time.time()
data_ls = []


DB_USER_NAME = "db"
DB_USER_PW = "pw"
DB_SEVER_ADDR = "192.168.0.10"
DB_DATABASE_NAME = "crawler"
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


async def handle_worker(group, task):
    """Handle amz_bsr_result task

    [input] task data format:
        JSON:
            {
                #product info
                "extra": {
                    "bsr": {
                        "bs_cate": [item["cate"]],
                        "date": "xxxx-xx-xx"
                    }
                }
            }
    [output] result data format:
        JSON:
            {
                #product info
                +"bs_cate": "cate",
                +"date": "2017-09-10",
                -"relative_info",
                -"extra",
            }
    """
    tp = TaskProtocal(task)
    task_dct = tp.get_data()

    if task_dct['relative_info']['bought_together'] or task_dct['relative_info']['also_bought']:
        bought_together_ls = task_dct['relative_info']['bought_together']
        also_bought_ls = task_dct['relative_info']['also_bought']
        time_now = datetime.strptime(task_dct['extra']['bsr']['date'], "%Y-%m-%d")
        for i in range(len(bought_together_ls)):
            asin = bought_together_ls[i]
            data_ls.append([task_dct['platform'], task_dct['asin'], 1, asin, i, time_now])
        for i in range(len(also_bought_ls)):
            asin = also_bought_ls[i]
            data_ls.append([task_dct['platform'], task_dct['asin'], 2, asin, i, time_now])
        if len(data_ls) > BATCH_SIZE:
            flush_to_db()

    info = task_dct
    popt_dct = popt_map.get(info['platform'], {})
    cat_name = info['detail_info'].get('cat_1_name', '').strip().lower()
    cat_rank = info['detail_info'].get('cat_1_rank', -1)
    info['detail_info']['cat_1_sales'] = -1
    if cat_name and cat_rank != -1 and popt_dct:
        info['detail_info']['cat_1_sales'] = CURVE_FUNC(cat_rank, *popt_dct.get(cat_name, popt_dct['default']))
    info['bs_cate'] = info['extra']['bsr']['bs_cate']
    info['date'] = info['extra']['bsr']['date']
    del info['relative_info']
    del info['extra']
    res = pipeflow.Task(json.dumps(info).encode('utf-8'))
    res.set_to('output')
    return res


def get_popt():
    global popt_map
    redis_client = redis.Redis(**POPT_REDIS_CONF)

    def redis_execute(func):
        @functools.wraps(func)
        def redis_execute_wrapper(*args, **kwargs):
            while True:
                try:
                    return func(*args, **kwargs)
                except redis.ConnectionError as e:
                    logger.error('Redis ConnectionError')
                    redis_client.connection_pool.disconnect()
                    continue
                except redis.TimeoutError as e:
                    logger.error('Redis TimeoutError')
                    redis_client.connection_pool.disconnect()
                    continue
        return redis_execute_wrapper

    key_ls = redis_execute(redis_client.keys)(POPT_KEY_PREFIX+'*')
    for key_name in key_ls:
        key_name = key_name.decode('utf-8')
        platform = key_name.replace(POPT_KEY_PREFIX, '')
        dct = redis_execute(redis_client.hgetall)(key_name)
        if dct:
            popt_map[platform] = {}
            for k,v in dct.items():
                k = k.decode('utf-8')
                v = v.decode('utf-8')
                popt_map[platform][k] = json.loads(v)
    redis_client.connection_pool.disconnect()


def flush_to_db():
    global last_flush_time
    db_session = db_session_mk()
    ar_ls = [AsinRelation(**dict(zip(["platform", "asin", "type", "rasin", "rank", "date"], item))) for item in data_ls]
    db_session.bulk_save_objects(ar_ls)
    try:
        db_session.commit()
    except:
        logger.error("flush db error")
        db_session.rollback()
    else:
        last_flush_time = time.time()
    finally:
        data_ls.clear()
        db_session.close()


async def crontab(server):
    while True:
        time_now = time.time()
        if data_ls and time_now > last_flush_time + FLUSH_INTERVAL:
            flush_to_db()
        await asyncio.sleep(FLUSH_INTERVAL)


async def del_expire(server):
    while True:
        await asyncio.sleep(DELETE_INTERVAL)
        date_expire = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d") - timedelta(day=EXPIRE_DAYS)
        db_session = db_session_mk()
        db_session.query(AsinRelation).filter(AsinRelation.date<date_expire).delete(synchronize_session=False)
        try:
            db_session.commit()
        except:
            logger.error("delete expire error")
            db_session.rollback()
        finally:
            db_session.close()


def run():
    get_popt()
    input_end = RabbitmqInputEndpoint('amz_bsr_result:input', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    output_end = pipeflow.RedisOutputEndpoint('amz_product:output:bsr', host='192.168.0.10', port=6379, db=5, password=None)

    server = pipeflow.Server()
    server.add_worker(crontab)
    server.add_worker(del_expire)
    group = server.add_group('main', 1)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end)
    server.run()
