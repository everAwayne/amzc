import json
import time
import redis
import socket
import asyncio
import pipeflow
from util.log import logger


RELATIONSHIP_REDIS_CONF = {'host': '192.168.0.10', 'port': 6379, 'db': 11, 'password': None}
RELATIONSHIP_KEY_PREFIX = "asin_rlts:"
BATCH_SIZE = 300
FLUSH_INTERVAL = 60
CACHE_LIFETIME_DAY = 14

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


def flush_to_cache(dct):
    pipeline = r.pipeline(transaction=False)
    for k, v in dct.items():
        k = RELATIONSHIP_KEY_PREFIX + k
        will_expire = max(v.values()) + 86400 * CACHE_LIFETIME_DAY
        has_expire = max(v.values()) - 86400 * CACHE_LIFETIME_DAY
        pipeline.zadd(k, **v)
        pipeline.zremrangebyscore(k, 0, has_expire)
        pipeline.expireat(k, will_expire)
    while True:
        try:
            pipeline.execute()
        except redis.ConnectionError as e:
            logger.error('Redis ConnectionError')
            r.connection_pool.disconnect()
            continue
        except redis.TimeoutError as e:
            logger.error('Redis TimeoutError')
            r.connection_pool.disconnect()
            continue
        else:
            global last_flush_time
            last_flush_time = time.time()
            dct.clear()
            break


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
        dct = data_dct.setdefault('%s:%s' % (task_dct['platform'], asin), {})
        dct[task_dct['asin']] = time_now
    if len(data_dct) > BATCH_SIZE:
        flush_to_cache(data_dct)

    return


async def crontab(server):
    while True:
        time_now = time.time()
        if data_dct and time_now > last_flush_time + FLUSH_INTERVAL:
            flush_to_cache(data_dct)

        await asyncio.sleep(FLUSH_INTERVAL)


def run():
    i_end = pipeflow.RedisInputEndpoint('amz_asin_relationship:input', host='192.168.0.10', port=6379, db=0, password=None)

    server = pipeflow.Server()
    server.add_worker(crontab)
    group = server.add_group('main', 1)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', i_end)
    server.run()
