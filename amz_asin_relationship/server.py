import json
import time
import redis
import socket
import asyncio
import functools
import pipeflow
from util.log import logger


RELATIONSHIP_REDIS_CONF = {'host': '192.168.0.10', 'port': 6379, 'db': 11, 'password': None}
RELATIONSHIP_KEY_PREFIX = "asin_rlts:"
BATCH_SIZE = 300
FLUSH_INTERVAL = 60
DEL_INTERVAL = 10
DEL_BATCH = 500
FREQUENCY_DAY = 7
CACHE_LIFETIME_DAY = 2 * FREQUENCY_DAY

conf = {
    "socket_timeout": 30,
    "socket_connect_timeout": 15,
    "socket_keepalive": True,
    "socket_keepalive_options": {
        socket.TCP_KEEPIDLE: 600,
        socket.TCP_KEEPCNT: 3,
        socket.TCP_KEEPINTVL: 60,
    }
}
conf.update(RELATIONSHIP_REDIS_CONF)
r = redis.Redis(**conf)
data_dct = {}
last_flush_time = time.time()


def redis_execute(func):
    @functools.wraps(func)
    def redis_execute_wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except redis.ConnectionError as e:
                logger.error('Redis ConnectionError')
                r.connection_pool.disconnect()
                continue
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                r.connection_pool.disconnect()
                continue

    return redis_execute_wrapper


def flush_to_cache(dct):
    pipeline = r.pipeline(transaction=False)
    for k, v in dct.items():
        pipeline.hmget(k, v.keys())
    ls = redis_execute(pipeline.execute)()

    for k, v in zip(dct.keys(), ls):
        result_dct = {}
        for asin, data in zip(dct[k].keys(), v):
            has_expire = max(dct[k][asin].values()) - 86400 * CACHE_LIFETIME_DAY
            if data is not None:
                cache_dct = json.loads(data.decode('utf-8'))
                cache_dct.update(dct[k][asin])
                for ck, cv in list(cache_dct.items()):
                    if cv <= has_expire:
                        del cache_dct[ck]
            else:
                cache_dct = dct[k][asin]
            result_dct[asin] = json.dumps(cache_dct)
        pipeline.hmset(k, result_dct)
    redis_execute(pipeline.execute)()


async def handle_worker(group, task):
    """Handle amz_asin_relationship task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "B02KDI8NID8",
                "date": "2017-08-08",
                "asin_ls": []
            }
    """
    task_dct = json.loads(task.get_data().decode('utf-8'))
    asin_ls = task_dct['asin_ls']
    time_now = int(time.mktime(time.strptime(task_dct['date'], "%Y-%m-%d")))
    for asin in asin_ls:
        dct = data_dct.setdefault(RELATIONSHIP_KEY_PREFIX + task_dct['platform'], {})
        dct = dct.setdefault(asin, {})
        dct[task_dct['asin']] = time_now
    if sum(map(lambda x:len(x), data_dct.values())) > BATCH_SIZE:
        flush_to_cache(data_dct)

    return


async def crontab(server):
    while True:
        time_now = time.time()
        if data_dct and time_now > last_flush_time + FLUSH_INTERVAL:
            flush_to_cache(data_dct)

        await asyncio.sleep(FLUSH_INTERVAL)


async def del_expire(server):
    while True:
        time_now = time.mktime(time.strptime(time.strftime("%Y-%m-%d", time.localtime()),"%Y-%m-%d"))
        has_expire = int(time_now) - 86400 * CACHE_LIFETIME_DAY
        key_ls = redis_execute(r.keys)(RELATIONSHIP_KEY_PREFIX+'*')
        for key in key_ls:
            cursor = None
            while cursor != 0:
                ret = redis_execute(r.hscan)(key, cursor=0 if cursor is None else cursor, count=DEL_BATCH)
                cursor = ret[0]
                ret_dct = ret[1]
                del_ls = []
                for k, v in ret_dct.items():
                    dct = json.loads(v.decode('utf-8'))
                    if max(dct.values()) <= has_expire:
                        del_ls.append(k)
                if del_ls:
                    redis_execute(r.hdel)(key, *del_ls)
                await asyncio.sleep(DEL_INTERVAL)
        await asyncio.sleep(86400 * FREQUENCY_DAY)


def run():
    i_end = pipeflow.RedisInputEndpoint('amz_asin_relationship:input', host='192.168.0.10', port=6379, db=0, password=None)

    server = pipeflow.Server()
    server.add_worker(crontab)
    server.add_worker(del_expire)
    group = server.add_group('main', 1)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', i_end)
    server.run()
