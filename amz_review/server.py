import re
import json
import time
import asyncio
import pipeflow
from urllib import parse
from error import RequestError, CaptchaError
from util.log import logger
from util.prrequest import get_page
from .spiders.dispatch import get_spider_by_platform, get_url_by_platform, formalize_url
from util.task_protocal import TaskProtocal
from pipeflow import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from config import RABBITMQ_CONF


MAX_WORKERS = 30

task_count = 0


async def handle_worker(group, task):
    """Handle amz_review task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "xxxx",
                "till": "reveiw id",
                "url": "xxxx",
            }
    [output] result data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "xxxx",
                "page": 1,
                "end": true,
                "reviews": [
                    {
                        "review_id": "xdf",
                        "rating": 4.0,
                        "title": "title",
                        "content": "content",
                        "author": "author",
                        "author_id": "author_id",
                        "date": "2017-09-09",
                        "verified_purchase": False,
                    }
                ]
            }
    """

    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    handle_cls = get_spider_by_platform(task_dct['platform'])
    notify_task = pipeflow.Task(b'task done')
    notify_task.set_to('notify')
    url = task_dct['url']
    if not url:
        url = get_url_by_platform(task_dct['platform'], task_dct['asin'])
    try:
        soup = await get_page(url, timeout=60)
        handle = handle_cls(soup)
    except RequestError:
        tp.set_to('inner_output')
        return tp
    except CaptchaError:
        tp.set_to('inner_output')
        return tp
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], url])
        logger.error('Get page handle error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return notify_task

    is_review_page = handle.is_review_page()
    # abandon result
    if not is_review_page:
        return notify_task

    try:
        page_info, review_ls = handle.get_info()
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], url])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return notify_task

    ### just for redirect response situation
    if page_info['cur_page_url']:
        pr = parse.urlparse(page_info['cur_page_url'])
        query_dct = dict(parse.parse_qsl(pr.query))
        if 'reviewerType' not in query_dct or 'pageSize' not in query_dct or 'sortBy' not in query_dct:
            new_url = get_url_by_platform(task_dct['platform'], task_dct['asin'], pr.path)
            task_dct['url'] = new_url
            new_tp = tp.new_task(task_dct)
            new_tp.set_to('inner_output')
            return new_tp

    if page_info['next_page_url']:
        page_info['next_page_url'] = formalize_url(task_dct['platform'], page_info['next_page_url'])
    review_id_ls = [item['review_id'] for item in review_ls]
    if 'till' in task_dct and task_dct['till'] in review_id_ls:
        page_info['next_page_url'] = None
        i = review_id_ls.index(task_dct['till'])
        review_ls = review_ls[:i]

    task_ls = []
    if page_info['next_page_url']:
        task_dct['url'] = page_info['next_page_url']
        new_tp = tp.new_task(task_dct)
        new_tp.set_to('inner_output')
        task_ls.append(new_tp)
    else:
        task_ls.append(notify_task)
    if review_ls:
        info = {
            'platform': task_dct['platform'], 'asin': task_dct['asin'],
            'page': page_info['cur_page'],
            'reviews': review_ls
        }
        if not page_info['next_page_url']:
            info['end'] = True
        new_tp = tp.new_task(info)
        new_tp.set_to('output')
        task_ls.append(new_tp)
    return task_ls


async def handle_task(group, task):
    """Handle amz_review task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "xxxx",
                "till": "reveiw id",
            }
    [notify] task data format:
        BYTES:
            b"task done"
    """
    global task_count

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
            logger.info("%s %s %s" % (task_dct['platform'], task_dct['asin'],
                                      task_dct.get('till', '')))
            task_dct["url"] = ""
            new_tp = tp.new_task(task_dct)
            new_tp.set_to('inner_output')
            return new_tp

    if from_name == 'notify' and task_count:
        if task.get_data() == b'task done':
            task_count -= 1
            if task_count+1 == MAX_WORKERS:
                group.resume_endpoint('input')


def run():
    input_end = RabbitmqInputEndpoint('amz_review:input', **RABBITMQ_CONF)
    back_end = RabbitmqOutputEndpoint('amz_review:input', **RABBITMQ_CONF)
    output_end = RabbitmqOutputEndpoint('amz_review:output', **RABBITMQ_CONF)
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
    task_group.add_output_endpoint('input_back', back_end)
    task_group.add_output_endpoint('inner_output', inner_output_end)

    worker_group = server.add_group('work', MAX_WORKERS)
    worker_group.set_handle(handle_worker)
    worker_group.add_input_endpoint('inner_input', inner_input_end)
    worker_group.add_output_endpoint('output', output_end)
    worker_group.add_output_endpoint('inner_output', inner_output_end)
    worker_group.add_output_endpoint('notify', notify_output_end)

    server.run()
