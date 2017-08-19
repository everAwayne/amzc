import json
import redis
import functools
import pipeflow
from error import RequestError, CaptchaError
from util.log import logger
from util.chrequest import get_page_handle, change_ip
from .spiders.dispatch import get_spider_by_platform, get_url_by_platform
from .modules import bsr, product


MAX_WORKERS = 15
POPT_REDIS_CONF = {'host': '192.168.0.10', 'port': 6379, 'db': 10, 'password': None}
POPT_KEY_PREFIX = "bestseller:popt:"


popt_map = {}


async def handle_worker(group, task):
    """Handle amz_product task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "B02KDI8NID8",
                "extra": {
                    "bs_cate": [],
                    "date": "2017-08-08"
                }
            }
    [output] result data format:
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
                'fba': 1,
                'review': 4.6,
                'review_count': 9812,
                'img': 'https://images-na.ssl-images-amazon.com/images/I/514RSPIJMKL.jpg'
            }
    """
    from_end = task.get_from()
    task_dct = json.loads(task.get_data().decode('utf-8'))

    logger.info("%s %s" % (task_dct['platform'], task_dct['asin']))

    handle_cls = get_spider_by_platform(task_dct['platform'])
    url = get_url_by_platform(task_dct['platform'], task_dct['asin'])
    try:
        handle = await get_page_handle(handle_cls, url, timeout=70)
    except RequestError:
        if from_end == 'input_bsr':
            task.set_to('loop_bsr')
        elif from_end == 'input_product':
            task.set_to('loop_product')
        return task
    except CaptchaError:
        if from_end == 'input_bsr':
            task.set_to('loop_bsr')
        elif from_end == 'input_product':
            task.set_to('loop_product')
        return task
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page handle error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return

    is_product_page = handle.is_product_page()
    if not is_product_page:
        return

    try:
        if from_end == 'input_bsr':
            return bsr.process(handle, task_dct, popt_map)
        elif from_end == 'input_product':
            return product.process(handle, task_dct)
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return


def get_popt():
    global popt_map
    popt_map.clear()
    r = redis.Redis(**POPT_REDIS_CONF)

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

    key_ls = redis_execute(r.keys)(POPT_KEY_PREFIX+'*')
    for key_name in key_ls:
        key_name = key_name.decode('utf-8')
        platform = key_name.replace(POPT_KEY_PREFIX, '')
        dct = redis_execute(r.hgetall)(key_name)
        if dct:
            popt_map[platform] = {}
            for k,v in dct.items():
                k = k.decode('utf-8')
                v = v.decode('utf-8')
                popt_map[platform][k] = json.loads(v)
    r.connection_pool.disconnect()


def run():
    get_popt()
    i_bsr_end = pipeflow.RedisInputEndpoint('amz_product:input:bsr', host='192.168.0.10', port=6379, db=0, password=None)
    o_bsr_end = pipeflow.RedisOutputEndpoint('amz_product:output:bsr', host='192.168.0.10', port=6379, db=0, password=None)
    l_bsr_end = pipeflow.RedisOutputEndpoint('amz_product:input:bsr', host='192.168.0.10', port=6379, db=0, password=None)
    o_rlts_end = pipeflow.RedisOutputEndpoint('amz_asin_relationship:input', host='192.168.0.10', port=6379, db=0, password=None)

    i_product_end = pipeflow.RedisInputEndpoint('amz_product:input:product', host='192.168.0.10', port=6379, db=0, password=None)
    o_product_end = pipeflow.RedisOutputEndpoint('amz_product:output:product', host='192.168.0.10', port=6379, db=0, password=None)
    l_product_end = pipeflow.RedisOutputEndpoint('amz_product:input:product', host='192.168.0.10', port=6379, db=0, password=None)

    server = pipeflow.Server()
    server.add_worker(change_ip)
    group = server.add_group('main', MAX_WORKERS)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input_bsr', i_bsr_end)
    group.add_output_endpoint('output_bsr', o_bsr_end)
    group.add_output_endpoint('loop_bsr', l_bsr_end)
    group.add_output_endpoint('output_rlts', o_rlts_end)

    group.add_input_endpoint('input_product', i_product_end)
    group.add_output_endpoint('output_product', o_product_end)
    group.add_output_endpoint('loop_product', l_product_end)
    server.run()
