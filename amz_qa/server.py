import re
import json
import time
import asyncio
import pipeflow
from error import RequestError, CaptchaError
from util.log import logger
from util.prrequest import get_page
from .spiders.dispatch import get_spider_by_platform, get_url_by_platform
from util.task_protocal import TaskProtocal
from util.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint


MAX_WORKERS = 3

task_count = 0


async def handle_worker(group, task):
    """Handle amz_qa task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "xxxx",
                "till": "qa id",
                "page": 1,
            }
    [output] result data format:
        JSON:
            {
                "platform": "amazon_us"
                "asin": "xxxx"
                "qas": [
                    {
                        "qa_id": "xdf",
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
    url = get_url_by_platform(task_dct['platform'], task_dct['asin'], task_dct['page'])
    logger.info("%s" % url)
    try:
        soup = await get_page(url, timeout=60)
        handle = handle_cls(soup)
    except RequestError:
        tp.set_to('inner_output')
        return tp.to_task()
    except CaptchaError:
        tp.set_to('inner_output')
        return tp.to_task()
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], url])
        logger.error('Get page handle error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return

    is_qa_page = handle.is_qa_page()
    # abandon result
    if not is_qa_page:
        return

    try:
        next_page, qa_ls = handle.get_info()
    except Exception as exc:
        exc_info = (type(exc), exc, exc.__traceback__)
        taks_info = ' '.join([task_dct['platform'], url])
        logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
        exc.__traceback__ = None
        return

    qa_id_ls = [item['qa_id'] for item in qa_ls]
    if 'till' in task_dct and task_dct['till'] in qa_id_ls:
        next_page = None
        i = qa_id_ls.index(task_dct['till'])
        qa_ls = qa_ls[:i]

    print(next_page, qa_ls)
    task_ls = []
    if next_page:
        task_dct['page'] = next_page
        new_tp = tp.new_task(task_dct)
        new_tp.set_to('inner_output')
        task_ls.append(new_tp.to_task())
    else:
        new_task = pipeflow.Task(b'task done')
        new_task.set_to('notify')
        task_ls.append(new_task)
    if qa_ls:
        info = {
            'platform': task_dct['platform'], 'asin': task_dct['asin'],
            'qas': qa_ls
        }
        new_tp = tp.new_task(info)
        new_tp.set_to('output')
        task_ls.append(new_tp.to_task())
    return task_ls


async def handle_task(group, task):
    """Handle amz_qa task

    [input] task data format:
        JSON:
            {
                "platform": "amazon_us",
                "asin": "xxxx",
                "till": "qa id",
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
            return tp.to_task()
        else:
            task_count += 1
            if task_count >= MAX_WORKERS:
                group.suspend_endpoint('input')
            task_dct = tp.get_data()
            logger.info("%s %s %s" % (task_dct['platform'], task_dct['asin'],
                                      task_dct.get('till', '')))
            task_dct["page"] = 1
            new_tp = tp.new_task(task_dct)
            new_tp.set_to('inner_output')
            return new_tp.to_task()

    if from_name == 'notify' and task_count:
        if task.get_data() == b'task done':
            task_count -= 1
            if task_count+1 == MAX_WORKERS:
                group.resume_endpoint('input')


def run():
    input_end = RabbitmqInputEndpoint('amz_qa:input', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    back_end = RabbitmqOutputEndpoint('amz_qa:input', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    output_end = RabbitmqOutputEndpoint('amz_qa:output', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    queue = asyncio.Queue()
    notify_input_end = pipeflow.QueueInputEndpoint(queue)
    notify_output_end = pipeflow.QueueOutputEndpoint(queue)
    queue = asyncio.Queue()
    inner_input_end = pipeflow.QueueInputEndpoint(queue)
    inner_output_end = pipeflow.QueueOutputEndpoint(queue)

    server = pipeflow.Server()

    task_group = server.add_group('task', MAX_WORKERS)
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
