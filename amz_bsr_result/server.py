import json
import math
import redis
import socket
import functools
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal
from util.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint


POPT_REDIS_CONF = {'host': '192.168.0.10', 'port': 6379, 'db': 10, 'password': None}
POPT_KEY_PREFIX = "bestseller:popt:"
CURVE_FUNC = lambda x,a,b:math.log(a/x+1)/b
popt_map = {}


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
                -"extra",
            }
    """
    tp = TaskProtocal(task)
    info = tp.get_data()
    popt_dct = popt_map.get(info['platform'], {})
    cat_name = info['detail_info'].get('cat_1_name', '').strip().lower()
    cat_rank = info['detail_info'].get('cat_1_rank', -1)
    info['detail_info']['cat_1_sales'] = -1
    if cat_name and cat_rank != -1 and popt_dct:
        info['detail_info']['cat_1_sales'] = CURVE_FUNC(cat_rank, *popt_dct.get(cat_name, popt_dct['default']))
    info['bs_cate'] = info['extra']['bsr']['bs_cate']
    info['date'] = info['extra']['bsr']['date']
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


def run():
    get_popt()
    input_end = RabbitmqInputEndpoint('amz_bsr_result:input', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    output_end = pipeflow.RedisOutputEndpoint('amz_product:output:bsr', host='192.168.0.10', port=6379, db=5, password=None)

    server = pipeflow.Server()
    group = server.add_group('main', 1)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end)
    server.run()
