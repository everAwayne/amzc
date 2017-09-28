import json
import time
import math
import redis
import socket
import asyncio
import functools
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal


POPT_REDIS_CONF = {'host': '192.168.0.10', 'port': 6379, 'db': 10, 'password': None}
POPT_KEY_PREFIX = "bestseller:popt:"
CURVE_FUNC = lambda x,a,b:math.log(a/x+1)/b
popt_map = {}


RELATIONSHIP_REDIS_CONF = {'host': '192.168.0.10', 'port': 6379, 'db': 11, 'password': None}
RELATIONSHIP_KEY_PREFIX = "asin_rlts:"
BATCH_SIZE = 2000
FLUSH_INTERVAL = 60
DEL_INTERVAL = 10
DEL_BATCH = 1000
FREQUENCY_DAY = 7
CACHE_LIFETIME_DAY = 2 * FREQUENCY_DAY

conf = {
    "socket_timeout": 40,
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


async def handle_worker(group, task):
    """Handle amz_bsr_result task

    [input] task data format:
        JSON:
            {
                'asin': 'B02KDI8NID8',
                'platform': 'amazon_us',
                'title': 'Active Wow Teeth Whitening Charcoal Powder Natural',
                'brand': 'Active Wow',
                'price': 24.79,
                'discount': 0.83,
                'merchant_id': 'A3RJPJ9XCKYOM5',
                'merchant': 'MarketWeb',
                'detail_info': {
                    'cat_1_rank': 5,
                    'cat_1_name': 'Beauty & Personal Care'
                },
                'relative_info': {
                    'bought_together': [],
                    'also_bought': [],
                },
                'fba': 1,
                'review': 4.6,
                'review_count': 9812,
                'img': 'https://images-na.ssl-images-amazon.com/images/I/514RSPIJMKL.jpg'
            }
    """
    tp = TaskProtocal(task)
    task_dct = tp.get_data()

    if task_dct['relative_info']['bought_together'] or task_dct['relative_info']['also_bought']:
        bought_together_ls = task_dct['relative_info']['bought_together']
        also_bought_ls = task_dct['relative_info']['also_bought']
        time_now = int(time.mktime(time.strptime(task_dct['extra']['bsr']['date'], "%Y-%m-%d")))
        for i in range(len(bought_together_ls)):
            asin = bought_together_ls[i]
            dct = data_dct.setdefault(RELATIONSHIP_KEY_PREFIX + 'bought_together:' + task_dct['platform'], {})
            dct = dct.setdefault(asin, {})
            dct[task_dct['asin']] = {'t': time_now, 'i': i}
        for i in range(len(also_bought_ls)):
            asin = also_bought_ls[i]
            dct = data_dct.setdefault(RELATIONSHIP_KEY_PREFIX + 'also_bought:' + task_dct['platform'], {})
            dct = dct.setdefault(asin, {})
            dct[task_dct['asin']] = {'t': time_now, 'i': i}
        if sum(map(lambda x:len(x), data_dct.values())) > BATCH_SIZE:
            flush_to_cache(data_dct)

    info = task_dct.copy()
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
    popt_map.clear()
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
            has_expire = max(map(lambda x:x['t'], dct[k][asin].values())) - 86400 * CACHE_LIFETIME_DAY
            if data is not None:
                cache_dct = json.loads(data.decode('utf-8'))
                cache_dct.update(dct[k][asin])
                for ck, cv in list(cache_dct.items()):
                    if cv['t'] <= has_expire:
                        del cache_dct[ck]
            else:
                cache_dct = dct[k][asin]
            result_dct[asin] = json.dumps(cache_dct)
        pipeline.hmset(k, result_dct)
    redis_execute(pipeline.execute)()

    global last_flush_time
    last_flush_time = time.time()
    dct.clear()


async def crontab(server):
    while True:
        time_now = time.time()
        if data_dct and time_now > last_flush_time + FLUSH_INTERVAL:
            flush_to_cache(data_dct)
        await asyncio.sleep(FLUSH_INTERVAL)


async def del_expire(server):
    while True:
        await asyncio.sleep(86400 * FREQUENCY_DAY)
        time_now = time.mktime(time.strptime(time.strftime("%Y-%m-%d", time.localtime()),"%Y-%m-%d"))
        has_expire = int(time_now) - 86400 * CACHE_LIFETIME_DAY
        key_ls = redis_execute(r.keys)(RELATIONSHIP_KEY_PREFIX+'*')
        for key in key_ls:
            if b'amazon_us' not in key:
                continue
            cursor = None
            while cursor != 0:
                ret = redis_execute(r.hscan)(key, cursor=0 if cursor is None else cursor, count=DEL_BATCH)
                cursor = ret[0]
                ret_dct = ret[1]
                del_ls = []
                for k, v in ret_dct.items():
                    dct = json.loads(v.decode('utf-8'))
                    if max(map(lambda x:x['t'], dct.values())) <= has_expire:
                        del_ls.append(k)
                if del_ls:
                    redis_execute(r.hdel)(key, *del_ls)
                await asyncio.sleep(DEL_INTERVAL)


def run():
    get_popt()
    input_end = pipeflow.RedisInputEndpoint('amz_bsr_result:input', host='192.168.0.10', port=6379, db=0, password=None)
    #output_end = pipeflow.RedisOutputEndpoint('amz_bsr_result:output', host='192.168.0.10', port=6379, db=0, password=None)
    output_end = pipeflow.RedisOutputEndpoint('amz_product:output:bsr', host='192.168.0.10', port=6379, db=0, password=None)

    server = pipeflow.Server()
    server.add_worker(crontab)
    server.add_worker(del_expire)
    group = server.add_group('main', 1)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end)
    server.run()
