import json
import asyncio
import pipeflow
from util.log import logger
from util.prrequest import get_page
from util.task_protocal import TaskProtocal
from error import RequestError, CaptchaError
from util.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from .spiders.url import get_search_index_url, get_host
from .spiders.page import AmazonSearchPage
from config import RABBITMQ_CONF


MAX_WORKERS = 30
task_count = 0


async def handle_worker(group, task):
    """
    """
    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    notify_task = pipeflow.Task(b'task done')
    notify_task.set_to('notify')
    if task_dct['page'] > task_dct['end_page']:
        return notify_task

    try:
        soup = await get_page(task_dct['url'], timeout=60)
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

    page = AmazonSearchPage(soup)
    task_ls = []
    host = get_host(task_dct['platform'])
    next_url = page.get_next_url(host)

    info = {
        'platform': task_dct['platform'],
        'keyword': task_dct['keyword'],
        'products': page.products,
        'right_products': page.right_products,
        'page': task_dct['page']
    }
    next_page = int(task_dct['page']) + 1
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
            if 'end_page' not in task_dct:
                task_dct['end_page'] = 20

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
    back_end = RabbitmqOutputEndpoint('amz_keyword:input', **RABBITMQ_CONF)
    output_end = RabbitmqOutputEndpoint('amz_keyword:output', **RABBITMQ_CONF)

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