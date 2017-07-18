import re
import json
import pipeflow
from error import RequestError
from util.log import logger
from util.chrequest import get_page_handle, change_ip
from .spiders.dispatch import get_spider_by_platform


MAX_WORKERS = 2


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

        handle_cls = get_spider_by_platform(task_dct['platform'])
        url = task_dct['root_url'] if 'root_url' in task_dct else task_dct['url']
        try:
            handle = await get_page_handle(handle_cls, url, timeout=90)
        except RequestError:
            if 'root_url' in task_dct:
                task_dct['url'] = task_dct['root_url']
                del task_dct['root_url']
                del task_dct['category_filter']
                task.set_data(json.dumps(task_dct).encode('utf-8'))
            task.set_to('inner_output')
            task_count += 1
            return task
        except Exception as exc:
            exc_info = (type(exc), exc, exc.__traceback__)
            taks_info = ' '.join(task_dct['platform'], url)
            logger.error('Get page handle error\n'+taks_info, exc_info=exc_info)
            exc.__traceback__ = None
            return

        is_bsr_page = handle.is_bsr_page()
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
            exc_info = (type(exc), exc, exc.__traceback__)
            taks_info = ' '.join(task_dct['platform'], url)
            logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
            exc.__traceback__ = None
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
