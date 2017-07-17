import os
import re
import json
import asyncio
import aiohttp
import pipeflow
from util.headers import get_header
from util.log import logger
from .spiders.dispatch import get_spider_by_platform


MAX_WORKERS = 2

# Two events to sync handle workers and change_ip worker
# handle worker wait for finish_change_event and set start_change_event
# change_ip worker wait for start_change_event and set finish_change_event
start_change_event = asyncio.Event()
finish_change_event = asyncio.Event()
start_change_event.clear()
finish_change_event.clear()
event_wait = 0
in_request = 0

filter_ls = []
task_start = False
task_count = 0
category_id_set = set([])
asin_set = set([])


async def handle_worker(server, task):
    """Handle amz_bsr_product task

    input task data format:
        JSON:
            {
                "platform": "amazon_us",
                "root_url": "https://www.amazon.de/gp/bestsellers",
                "category_filter": ["name1", ... ,"namex"]
            }
    """
    global event_wait
    global in_request
    global filter_ls
    global task_start
    global task_count
    global category_id_set
    global asin_set
    task_dct = json.loads(task.get_data().decode('utf-8'))

    if task_start and 'root_url' in task_dct:
        task.set_to('input_back')
        return task
    if not task_start:
        assert "root_url" in task_dct, "first task must come from input endpoint"
        server.suspend_endpoint('input')
        task_start = True
        filter_ls = [cate.lower() for cate in task_dct['category_filter']]

    try:
        if 'root_url' not in task_dct:
            task_count -= 1
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
            url = task_dct['root_url'] if 'root_url' in task_dct else task_dct['url']
            print(url)
            headers = get_header()
            html = None
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(url) as resp:
                        html = await resp.read()
            except Exception as exc:
                exc_info = (type(exception), exception, exception.__traceback__)
                logger.error('Request page fail', exc_info=exc_info)
                task.set_to('inner_output')
                task_count += 1
                return task
            try:
                handle = handle_cls(html)
            except Exception as exc:
                exc_info = (type(exception), exception, exception.__traceback__)
                taks_info = ' '.join(task_dct['platform'], url)
                logger.error('Parse page error\n'+taks_info, exc_info=exc_info)
                return
            is_bsr_page = handle.is_bsr_page()
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
            if 'root_url' in task_dct:
                task_dct['url'] = task_dct['root_url']
                del task_dct['root_url']
                del task_dct['category_filter']
                task.set_data(json.dumps(task_dct).encode('utf-8'))
            task.set_to('inner_output')
            task_count += 1
            return task

        # abandon result
        if not is_bsr_page:
            return

        is_exist = False
        reg = re.search(r'/(\d+)/ref=', url)
        if reg:
            cate_id = reg.group(1)
            if cate_id not in category_id_set:
                category_id_set.add(cate_id)
            else:
                is_exist = True
        try:
            url_ls, asin_ls = handle.get_info(filter_ls, is_exist)
        except Exception as exc:
            exc_info = (type(exception), exception, exception.__traceback__)
            taks_info = ' '.join(task_dct['platform'], url)
            logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
            return
        asin_ls = [item for item in asin_ls if item['asin'] not in asin_set]
        asin_set.update([item['asin'] for item in asin_ls])

        task_ls = []
        for url in url_ls:
            new_task = pipeflow.Task(json.dumps({'platform': task_dct['platform'],
                                                 'url': url}).encode('utf-8'))
            new_task.set_to('inner_output')
            task_ls.append(new_task)
        task_count += len(url_ls)
        for item in asin_ls:
            new_task = pipeflow.Task(json.dumps({'platform': task_dct['platform'],
                                                 'asin': item['asin'],
                                                 'extra': item['cate']}).encode('utf-8'))
            new_task.set_to('output')
            task_ls.append(new_task)
        return task_ls
    finally:
        if task_start and server.get_running_cnt() == 1 and task_count == 0:
            filter_ls = []
            task_start = False
            category_id_set = set([])
            asin_set = set([])
            server.resume_endpoint('input')


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
    bsr_end = pipeflow.RedisInputEndpoint('bsr_end', host='192.168.0.10', port=6379, db=0, password=None)
    back_end = pipeflow.RedisOutputEndpoint('bsr_end', host='192.168.0.10', port=6379, db=0, password=None)
    product_end = pipeflow.RedisOutputEndpoint('product_end', host='192.168.0.10', port=6379, db=0, password=None)
    queue = asyncio.LifoQueue()
    inner_input_end = pipeflow.QueueInputEndpoint(queue)
    inner_output_end = pipeflow.QueueOutputEndpoint(queue)

    server = pipeflow.Server(MAX_WORKERS)
    server.set_handle(handle_worker)
    server.add_worker(change_ip)
    server.add_input_endpoint('input', bsr_end)
    server.add_input_endpoint('inner_input', inner_input_end)
    server.add_output_endpoint('output', product_end, buffer_size=MAX_WORKERS*20)
    server.add_output_endpoint('input_back', back_end)
    server.add_output_endpoint('inner_output', inner_output_end)
    server.run()
