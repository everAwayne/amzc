import re
import json
import asyncio
import pipeflow
from error import RequestError
from util.log import logger
from util.chrequest import get_page_handle, change_ip
from .spiders.dispatch import get_spider_by_platform


MAX_WORKERS = 5


filter_ls = []
task_start = False
task_count = 0
category_id_set = set([])
asin_set = set([])


async def handle_worker(group, task):
    """Handle amz_bsr_product task

    [inner_input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "url": "https://www.amazon.de/gp/bestsellers",
            }
    """
    global filter_ls
    global task_count
    global category_id_set
    global asin_set
    task_dct = json.loads(task.get_data().decode('utf-8'))

    try:
        task_count -= 1
        handle_cls = get_spider_by_platform(task_dct['platform'])
        url = task_dct['url']
        try:
            handle = await get_page_handle(handle_cls, url, timeout=90)
        except RequestError:
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
            cate_id = int(reg.group(1))
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
        if group.get_running_cnt() == 1 and task_count == 0:
            category_id_set = set([])
            asin_set = set([])
            new_task = pipeflow.Task(b'task done')
            new_task.set_to('notify')
            return new_task


async def handle_task(group, task):
    """Handle amz_bsr_product task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "root_url": "https://www.amazon.de/gp/bestsellers",
                "category_filter": ["name1", ... ,"namex"]
            }
    [notify] task data format:
        BYTES:
            b"task done"
    """
    global filter_ls
    global task_start
    global task_count

    from_name = task.get_from()
    if from_name == 'input':
        if task_start:
            task.set_to('input_back')
            return task
        else:
            group.suspend_endpoint('input')
            task_start = True
            task_dct = json.loads(task.get_data().decode('utf-8'))
            filter_ls = [cate.lower() for cate in task_dct['category_filter']]
            task_dct['url'] = task_dct['root_url']
            del task_dct['root_url']
            del task_dct['category_filter']
            task.set_data(json.dumps(task_dct).encode('utf-8'))
            task.set_to('inner_output')
            task_count = 1
            return task

    if from_name == 'notify' and task_start:
        if task.get_data() == b'task done':
            filter_ls = []
            task_start = False
            group.resume_endpoint('input')


def run():
    bsr_end = pipeflow.RedisInputEndpoint('bsr_end', host='192.168.0.10', port=6379, db=0, password=None)
    back_end = pipeflow.RedisOutputEndpoint('bsr_end', host='192.168.0.10', port=6379, db=0, password=None)
    queue = asyncio.Queue()
    notify_input_end = pipeflow.QueueInputEndpoint(queue)
    notify_output_end = pipeflow.QueueOutputEndpoint(queue)
    product_end = pipeflow.RedisOutputEndpoint('product_end', host='192.168.0.10', port=6379, db=0, password=None)
    queue = asyncio.LifoQueue()
    inner_input_end = pipeflow.QueueInputEndpoint(queue)
    inner_output_end = pipeflow.QueueOutputEndpoint(queue)

    server = pipeflow.Server()
    server.add_worker(change_ip)

    task_group = server.add_group('task', 1)
    task_group.set_handle(handle_task)
    task_group.add_input_endpoint('input', bsr_end)
    task_group.add_input_endpoint('notify', notify_input_end)
    task_group.add_output_endpoint('input_back', back_end)
    task_group.add_output_endpoint('inner_output', inner_output_end)

    worker_group = server.add_group('work', MAX_WORKERS)
    worker_group.set_handle(handle_worker)
    worker_group.add_input_endpoint('inner_input', inner_input_end)
    worker_group.add_output_endpoint('output', product_end, buffer_size=MAX_WORKERS*20)
    worker_group.add_output_endpoint('inner_output', inner_output_end)
    worker_group.add_output_endpoint('notify', notify_output_end)
    server.run()
