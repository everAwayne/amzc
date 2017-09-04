import json
import pipeflow
from error import RequestError, CaptchaError
from util.log import logger
from util.chrequest import get_page_handle, change_ip
from util.task_protocal import TaskProtocal
from .spiders.dispatch import get_spider_by_platform, get_url_by_platform


MAX_WORKERS = 15


async def handle_worker(group, task):
    """Handle amz_product task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "B02KDI8NID8"
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
    from_end = tp.get_from()
    task_dct = tp.get_data()
    logger.info("%s %s" % (task_dct['platform'], task_dct['asin']))

    handle_cls = get_spider_by_platform(task_dct['platform'])
    url = get_url_by_platform(task_dct['platform'], task_dct['asin'])
    try:
        handle = await get_page_handle(handle_cls, url, timeout=70)
    except RequestError:
        if from_end == 'routine_input':
            tp.set_to('routine_input')
        elif from_end == 'input':
            tp.set_to('input')
        return tp.to_task()
    except CaptchaError:
        if from_end == 'routine_input':
            tp.set_to('routine_input')
        elif from_end == 'input':
            tp.set_to('input')
        return tp.to_task()
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
        info = handle.get_info()
        # extra info
        info['asin'] = task_dct['asin']
        info['platform'] = task_dct['platform']
        if task_dct.get('extra'):
            info['extra'] = task_dct['extra']
        new_tp = tp.new_task(info)
        new_tp.set_to('output')
        return new_tp.to_task()
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return


def run():
    routine_input_end = pipeflow.RedisInputEndpoint('amz_product:routine_input', host='192.168.0.10', port=6379, db=0, password=None)
    b_routine_input_end = pipeflow.RedisOutputEndpoint('amz_product:routine_input', host='192.168.0.10', port=6379, db=0, password=None)
    input_end = pipeflow.RedisInputEndpoint('amz_product:input', host='192.168.0.10', port=6379, db=0, password=None)
    b_input_end = pipeflow.RedisOutputEndpoint('amz_product:input', host='192.168.0.10', port=6379, db=0, password=None)
    output_end = pipeflow.RedisOutputEndpoint('amz_product:output', host='192.168.0.10', port=6379, db=0, password=None)

    server = pipeflow.Server()
    server.add_worker(change_ip)
    group = server.add_group('main', MAX_WORKERS)
    group.set_handle(handle_worker)
    group.add_input_endpoint('routine_input', routine_input_end)
    group.add_output_endpoint('routine_input_back', b_routine_input_end)

    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('input_back', b_input_end)
    group.add_output_endpoint('output', output_end)
    server.run()
