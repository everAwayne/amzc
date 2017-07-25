import json
import pipeflow
from error import RequestError
from util.log import logger
from util.chrequest import get_page_handle, change_ip
from .spiders.dispatch import get_spider_by_platform, get_url_by_platfrom


MAX_WORKERS = 15

task_cnt = 0


async def handle_worker(group, task):
    """Handle amz_product task

    [input] task data format:
        JSON:
            {
                "platfrom": "amazon_us",
                "asin": "B02KDI8NID8",
                "extra": {}         #optional
            }
    [output] result data format:
        JSON:
            {
                'asin': 'B02KDI8NID8',
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
    global task_cnt
    from_end = task.get_from()
    task_dct = json.loads(task.get_data().decode('utf-8'))

    #task_cnt += 1
    logger.info("%s %s" % (task_dct['platform'], task_dct['asin']))

    handle_cls = get_spider_by_platform(task_dct['platform'])
    url = get_url_by_platfrom(task_dct['platform'], task_dct['asin'])
    try:
        handle = await get_page_handle(handle_cls, url, timeout=60)
    except RequestError:
        if from_end == 'input_bsr':
            task.set_to('loop_bsr')
        return task
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page handle error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return

    is_product_page = handle.is_product_page()
    # abandon result
    if not is_product_page:
        return

    try:
        info = handle.get_info()
        info['asin'] = task_dct['asin']
        info['platfrom'] = task_dct['platform']
        info.update(task_dct['extra'])
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return

    result_task = pipeflow.Task(json.dumps(info).encode('utf-8'))
    if from_end == 'input_bsr':
        result_task.set_to('output_bsr')
    return result_task


def run():
    i_bsr_end = pipeflow.RedisInputEndpoint('amz_product:input:bsr', host='192.168.0.10', port=6379, db=0, password=None)
    o_bsr_end = pipeflow.RedisOutputEndpoint('amz_product:output:bsr', host='192.168.0.10', port=6379, db=0, password=None)
    l_bsr_end = pipeflow.RedisOutputEndpoint('amz_product:input:bsr', host='192.168.0.10', port=6379, db=0, password=None)

    server = pipeflow.Server()
    server.add_worker(change_ip)
    group = server.add_group('main', MAX_WORKERS)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input_bsr', i_bsr_end)
    group.add_output_endpoint('output_bsr', o_bsr_end)
    group.add_output_endpoint('loop_bsr', l_bsr_end)
    server.run()
