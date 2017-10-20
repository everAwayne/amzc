import re
import json
import time
import asyncio
import pipeflow
from error import RequestError, CaptchaError
from util.log import logger
from util.prrequest import get_page
from .spiders.dispatch import get_spider_by_platform
from util.task_protocal import TaskProtocal
from util.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint


MAX_WORKERS = 3


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
                "date": "2017-08-08"
            }
    [output] result data format:
        JSON:
            {
                "platform": "amazon_us"
                "asin": "xxxx"
                "extra": {
                    "bsr": {
                        "bs_cate": [item["cate"]],
                        "date": "xxxx-xx-xx"
                    }
                }
            }
    """
    global filter_ls
    global task_count
    global category_id_set
    global asin_set

    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    try:
        task_count -= 1
        handle_cls = get_spider_by_platform(task_dct['platform'])
        url = task_dct['url']
        logger.info("%s" % (url,))
        try:
            soup = await get_page(url, timeout=60)
            handle = handle_cls(soup)
        except RequestError:
            tp.set_to('inner_output')
            task_count += 1
            return tp.to_task()
        except CaptchaError:
            tp.set_to('inner_output')
            task_count += 1
            return tp.to_task()
        except Exception as exc:
            exc_info = (type(exc), exc, exc.__traceback__)
            taks_info = ' '.join([task_dct['platform'], url])
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
            taks_info = ' '.join([task_dct['platform'], url])
            logger.error('Get page info error\n'+taks_info, exc_info=exc_info)
            exc.__traceback__ = None
            return
        asin_ls = [item for item in asin_ls if item['asin'] not in asin_set]
        asin_set.update([item['asin'] for item in asin_ls])

        task_ls = []
        for url in url_ls:
            new_tp = tp.new_task({'platform': task_dct['platform'], 'url': url, 'date': task_dct['date']})
            new_tp.set_to('inner_output')
            task_ls.append(new_tp.to_task())
        task_count += len(url_ls)
        for item in asin_ls:
            new_tp = tp.new_task({'platform': task_dct['platform'],
                                  'asin': item['asin'],
                                  'extra': {
                                      'bsr': {'bs_cate': [item['cate']], 'date': task_dct['date']}
                                      }
                                  })
            new_tp.set_to('output')
            task_ls.append(new_tp.to_task())
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
        tp = TaskProtocal(task)
        if task_start:
            tp.set_to('input_back')
            return tp.to_task()
        else:
            group.suspend_endpoint('input')
            task_start = True
            task_dct = tp.get_data()
            logger.info(task_dct['root_url'])
            filter_ls = [cate.lower() for cate in task_dct['category_filter']]
            task_dct['url'] = task_dct['root_url']
            task_dct['date'] = time.strftime("%Y-%m-%d", time.localtime())
            del task_dct['root_url']
            del task_dct['category_filter']
            new_tp = tp.new_task(task_dct)
            new_tp.set_to('inner_output')
            task_count = 1
            return new_tp.to_task()

    if from_name == 'notify' and task_start:
        if task.get_data() == b'task done':
            filter_ls = []
            task_start = False
            group.resume_endpoint('input')


def run():
    bsr_end = RabbitmqInputEndpoint('amz_bsr:input', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    back_end = RabbitmqOutputEndpoint('amz_bsr:input', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    product_end = RabbitmqOutputEndpoint('amz_bsr:output', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    queue = asyncio.Queue()
    notify_input_end = pipeflow.QueueInputEndpoint(queue)
    notify_output_end = pipeflow.QueueOutputEndpoint(queue)
    queue = asyncio.LifoQueue()
    inner_input_end = pipeflow.QueueInputEndpoint(queue)
    inner_output_end = pipeflow.QueueOutputEndpoint(queue)

    server = pipeflow.Server()

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
