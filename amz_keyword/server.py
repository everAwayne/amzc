import json
import asyncio
import pipeflow
from lxml import etree
from util.log import logger
from util.prrequest import GetPageSession
from util.task_protocal import TaskProtocal
from error import RequestError, CaptchaError
from pipeflow import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from .spiders.dispatch import get_spider_by_platform, get_search_index_url, formalize_url
from config import RABBITMQ_CONF


MAX_WORKERS = 30
task_count = 0


async def handle_worker(group, task):
    """Handle amz_keyword task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "keyword": "xx xxx",
                "end_page": 10,
                "page": 1,
                "url": "xxxx",
            }
    [output] result data format:
        JSON:
            {
                "platform": "amazon_us",
                "keyword": "xx xxx",
                "page": 1,
                "end": true,
                "status": 0,
                "products": [
                    {'is_sponsored': 1, 'rank': 1, 'asin': 'B073S6F9JQ'}
                ],
                "count": 10,
                "category": ['xxx1','xx2'],
                "department": "xxxxx",
            }
    """

    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    notify_task = pipeflow.Task(b'task done')
    notify_task.set_to('notify')
    if task_dct['page'] > task_dct['end_page']:
        return notify_task
    handle_cls = get_spider_by_platform(task_dct['platform'])

    try:
        sess = GetPageSession()
        html = await sess.get_page('get', task_dct['url'], timeout=60, captcha_bypass=True)
        soup = etree.HTML(html, parser=etree.HTMLParser(encoding='utf-8'))
        handle = handle_cls(soup)
    except RequestError:
        tp.set_to('inner_output')
        return tp
    except CaptchaError:
        tp.set_to('inner_output')
        return tp
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['url']])
        logger.error('Get page handle error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None

        tps = [notify_task]
        new_tp = tp.new_task({
            'platform': task_dct['platform'],
            'keyword': task_dct['keyword'],
            'page': task_dct['page'],
            'end': True,
            'status': 1,
            'message': 'Get Page handle error'
        })
        new_tp.set_to('output')
        tps.append(new_tp)
        return tps

    is_search_page = handle.is_search_page()
    if not is_search_page:
        return notify_task

    try:
        next_url = handle.get_next_url()
        asin_ls = handle.get_asins()
        result_dct = handle.get_search_result()
        department = handle.get_nav_search()
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], task_dct['url']])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return notify_task

    if next_url is not None:
        next_url = formalize_url(task_dct['platform'], next_url)

    task_ls = []
    info = {
        'platform': task_dct['platform'],
        'keyword': task_dct['keyword'],
        'page': task_dct['page'],
        'products': asin_ls,
        'count': result_dct['count'],
        'category': result_dct['category'],
        'department': department,
    }
    next_page = task_dct['page'] + 1
    if next_url and next_page <= task_dct['end_page']:
        task_dct['page'] = next_page
        task_dct['url'] = next_url
        new_tp = tp.new_task(task_dct)
        new_tp.set_to('inner_output')
        task_ls.append(new_tp)
    else:
        info['end'] = True
        info['status'] = 0
        task_ls.append(notify_task)

    new_tp = tp.new_task(info)
    new_tp.set_to('output')
    task_ls.append(new_tp)
    return task_ls


async def handle_task(group, task):
    """Handle amz_keyword task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "keyword": "xx xxx",
                "end_page": 10,
            }
    [notify] task data format:
        BYTES:
            b"task done"
    """
    global  task_count

    from_name = task.get_from()
    if from_name == 'input':
        tp = TaskProtocal(task)
        if task_count >= MAX_WORKERS:
            tp.set_to('input_back')
            return tp
        else:
            task_count += 1
            if task_count >= MAX_WORKERS:
                group.suspend_endpoint('input')
            task_dct = tp.get_data()
            logger.info("%s %s" % (task_dct['platform'], task_dct['keyword']))
            task_dct.setdefault('end_page', 20)
            task_dct['page'] = 1
            task_dct['url'] = get_search_index_url(task_dct['platform'], task_dct['keyword'])
            new_tp = tp.new_task(task_dct)
            new_tp.set_to('inner_output')
            return new_tp

    if from_name == 'notify' and task_count:
        if task.get_data() == b'task done':
            task_count -= 1
            if task_count+1 == MAX_WORKERS:
                group.resume_endpoint('input')

def run():
    input_end = RabbitmqInputEndpoint('amz_keyword:input', **RABBITMQ_CONF)
    output_end = RabbitmqOutputEndpoint(['amz_keyword:input', 'amz_keyword:output'], **RABBITMQ_CONF)

    queue = asyncio.Queue()
    notify_input_end = pipeflow.QueueInputEndpoint(queue)
    notify_output_end = pipeflow.QueueOutputEndpoint(queue)
    queue = asyncio.Queue()
    inner_input_end = pipeflow.QueueInputEndpoint(queue)
    inner_output_end = pipeflow.QueueOutputEndpoint(queue)

    server = pipeflow.Server()

    task_group = server.add_group('task', 1)
    task_group.set_handle(handle_task)
    task_group.add_input_endpoint('input', input_end)
    task_group.add_input_endpoint('notify', notify_input_end)
    task_group.add_output_endpoint('input_back', output_end, 'amz_keyword:input')
    task_group.add_output_endpoint('inner_output', inner_output_end)

    worker_group = server.add_group('work', MAX_WORKERS)
    worker_group.set_handle(handle_worker)
    worker_group.add_input_endpoint('inner_input', inner_input_end)
    worker_group.add_output_endpoint('output', output_end, 'amz_keyword:output')
    worker_group.add_output_endpoint('inner_output', inner_output_end)
    worker_group.add_output_endpoint('notify', notify_output_end)

    server.run()
