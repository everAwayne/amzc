import json
import pipeflow
from error import RequestError, CaptchaError
from util.log import logger
from util.prrequest import get_page
from util.task_protocal import TaskProtocal
from .spiders.dispatch import get_spider_by_platform, get_url_by_platform
from util.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from config import RABBITMQ_CONF


MAX_WORKERS = 40


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
                "product_info": {
                    "product_dimensions": "2 x 2 x 2 inches ; 0.6 ounces",
                    "shipping_weight": "3.2 ounces ()",
                    "date_first_available": null
                },
                'detail_info': {
                    'cat_1_rank': 5,
                    'cat_1_name': 'Beauty & Personal Care',
                    "cat_ls": [{"rank": 4, "name_ls": ["Health & Household", "Oral Care", "Teeth Whitening"]}],
                },
                'relative_info': {
                    'bought_together': [],
                    'also_bought': [],
                    'also_viewed': [],
                    'viewed_also_bought': [],
                },
                'fba': 1,
                'review': 4.6,
                'review_count': 9812,
                "review_statistics": {
                    "1": 6,
                    "2": 2,
                    "3": 3,
                    "4": 9,
                    "5": 80
                },
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
        soup = await get_page(url, timeout=70)
        handle = handle_cls(soup)
    except RequestError:
        tp.set_to('input_back')
        return tp
    except CaptchaError:
        tp.set_to('input_back')
        return tp
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
        new_tp = tp.new_task(info)
        new_tp.set_to('output')
        return new_tp
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['asin']])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return


def run():
    input_end = RabbitmqInputEndpoint('amz_product:input', **RABBITMQ_CONF)
    b_input_end = RabbitmqOutputEndpoint('amz_product:input', **RABBITMQ_CONF)
    output_end = RabbitmqOutputEndpoint('amz_product:output', **RABBITMQ_CONF)

    server = pipeflow.Server()
    group = server.add_group('main', MAX_WORKERS)
    group.set_handle(handle_worker)
    group.add_input_endpoint('input', input_end)
    group.add_output_endpoint('input_back', b_input_end)
    group.add_output_endpoint('output', output_end)
    server.run()
