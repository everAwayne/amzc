import json
import time
import math
import redis
import socket
import functools
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal
from pipeflow import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from config import RABBITMQ_CONF, REDIS_CONF, BSR_REDIS_CONF


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
    cat_name = info['detail_info']['cat_1_name'].strip().lower() if info['detail_info']['cat_1_name'] else ''
    cat_rank = info['detail_info']['cat_1_rank'] if info['detail_info']['cat_1_rank'] is not None else -1
    info['detail_info']['cat_1_sales'] = -1
    if cat_name and cat_rank != -1 and popt_dct:
        info['detail_info']['cat_1_sales'] = CURVE_FUNC(cat_rank, *popt_dct.get(cat_name, popt_dct['default']))
    if info.get('extra') and info['extra'].get('bsr'):
        info['bs_cate'] = info['extra']['bsr']['bs_cate']
        info['date'] = info['extra']['bsr']['date']
        del info['extra']
    else:
        cate = ''
        if info['detail_info']['cat_ls']:
            cate = ':'.join(info['detail_info']['cat_ls'][0]['name_ls'])
        info['bs_cate'] = [cate]
        info['date'] = time.strftime("%Y-%m-%d", time.localtime())
    res = pipeflow.Task(json.dumps(info).encode('utf-8'))
    res.set_to('output')
    return res


def get_popt():
    global popt_map
    redis_client = redis.Redis(**REDIS_CONF)

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
    input_end = RabbitmqInputEndpoint('amz_bsr_result:input', **RABBITMQ_CONF)
    output_end = pipeflow.RedisOutputEndpoint('amz_product:output:bsr', **BSR_REDIS_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', 1)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('output', output_end)
    server.run()
