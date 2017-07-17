import os
import json
import asyncio
import aiohttp
import pipeflow
from util.headers import get_header
from util.log import logger
from .spiders.dispatch import get_spider_by_platform, get_url_by_platfrom


MAX_WORKERS = 5

# Two events to sync handle workers and change_ip worker
# handle worker wait for finish_change_event and set start_change_event
# change_ip worker wait for start_change_event and set finish_change_event
start_change_event = asyncio.Event()
finish_change_event = asyncio.Event()
start_change_event.clear()
finish_change_event.clear()
event_wait = 0
in_request = 0

async def handle_worker(server, task):
    """Handle amz_product task

    input task data format:
        JSON:
            {
                "platfrom": "amazon_us",
                "asin": "B02KDI8NID8"
            }
    output result data format:
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
    global event_wait
    global in_request
    task_dct = json.loads(task.get_data().decode('utf-8'))

    try:
        in_request += 1
        # if ther is any handle worker is waiting for changing ip,
        # or have received a captcha page,
        # then stop and wait for changing ip
        # trigger to change ip when all running handle worker have no IO operation
        if event_wait > 0:
            if event_wait+1 == in_request:
                start_change_event.set()
                start_change_event.clear()
            event_wait += 1
            await finish_change_event.wait()

        handle_cls = get_spider_by_platform(task_dct['platform'])
        url = get_url_by_platfrom(task_dct['platform'], task_dct['asin'])
        headers = get_header()
        html = None
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=90) as resp:
                    html = await resp.read()
        except Exception as exc:
            exc_info = (type(exception), exception, exception.__traceback__)
            logger.error('Request page fail', exc_info=exc_info)
            task.set_to('loop')
            return task
        try:
            handle = handle_cls(html, task_dct['asin'])
        except Exception as exc:
            exc_info = (type(exception), exception, exception.__traceback__)
            taks_info = ' '.join(task_dct['platform'], task_dct['asin'])
            logger.error('Parse page error\n'+taks_info, exc_info=exc_info)
            return
        is_product_page = handle.is_product_page()
        is_captcha_page = handle.is_captcha_page()

        if is_captcha_page:
            if event_wait+1 == in_request:
                start_change_event.set()
                start_change_event.clear()
            event_wait += 1
            await finish_change_event.wait()
    finally:
        in_request -= 1
        if event_wait > 0 and event_wait == in_request:
            start_change_event.set()
            start_change_event.clear()

    # redo task
    if is_captcha_page:
        task.set_to('loop')
        return task

    # abandon result
    if not is_product_page:
        return

    try:
        info = handle.get_info()
    except Exception as exc:
        exc_info = (type(exception), exception, exception.__traceback__)
        taks_info = ' '.join(task_dct['platform'], task_dct['asin'])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        return

    result_task = pipeflow.Task(json.dumps(info).encode('utf-8'))
    result_task.set_to('output')
    return result_task


async def change_ip(server):
    """Change machine IP(blocking)

    Wait for start_change_event.
    When all running handle workers are wait for finish_change_event,
    start_change_event will be set.
    """
    global event_wait
    global in_request
    while True:
        await start_change_event.wait()
        assert event_wait, "event_wait should more than 0"
        assert event_wait==in_request, "event_wait should be equal to running_cnt"
        while True:
            ret = os.system('ifdown ppp0')
            if ret == 0:
                break
            else:
                logger.error('ppp0 stop error')
                asyncio.sleep(10)
        while True:
            ret = os.system('ifup ppp0')
            if ret == 0:
                break
            else:
                logger.error('ppp0 start error')
                asyncio.sleep(10)
        finish_change_event.set()
        finish_change_event.clear()
        event_wait = 0


def run():
    i_end = pipeflow.RedisInputEndpoint('i_end', host='192.168.0.10', port=6379, db=0, password=None)
    o_end = pipeflow.RedisOutputEndpoint('o_end', host='192.168.0.10', port=6379, db=0, password=None)
    l_end = pipeflow.RedisOutputEndpoint('i_end', host='192.168.0.10', port=6379, db=0, password=None)

    server = pipeflow.Server(MAX_WORKERS)
    server.set_handle(handle_worker)
    server.add_worker(change_ip)
    server.add_input_endpoint('input', i_end)
    server.add_output_endpoint('output', o_end)
    server.add_output_endpoint('loop', l_end)
    server.run()
