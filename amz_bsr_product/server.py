import re
import json
import time
import asyncio
import pipeflow
from lxml import etree
from error import RequestError, CaptchaError, BannedError
from util.log import logger
from util.prrequest import GetPageSession
from .spiders.dispatch import get_spider_by_platform
from util.task_protocal import TaskProtocal
from pipeflow import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from config import RABBITMQ_CONF


MAX_WORKERS = 1
PLATFORM_FILTER_LS = ["amazon_us"]


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
                "with_qty": True    #optional
            }
    [output] result data format:
        JSON:
            {
                "platform": "amazon_us"
                "asin": "xxxx"
                "with_qty": True    #optional
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
        with GetPageSession() as sess:
            try:
                #sess = GetPageSession()
                html = await sess.get_page('get', url, timeout=60, captcha_bypass=True)
                soup = etree.HTML(html, parser=etree.HTMLParser(encoding='utf-8'))
                handle = handle_cls(soup)
            except BannedError as exc:
                tp.set_to('input_back')
                ban_tp = tp.new_task({'proxy': exc.proxy[7:]})
                ban_tp.set_to('ban')
                return [ban_tp, tp]
            except RequestError:
                tp.set_to('inner_output')
                task_count += 1
                return tp
            except CaptchaError:
                tp.set_to('inner_output')
                task_count += 1
                return tp
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
            new_tp = tp.new_task({'platform': task_dct['platform'], 'url': url,
                                  'date': task_dct['date'],
                                  'with_qty': task_dct.get('with_qty', False)})
            new_tp.set_to('inner_output')
            task_ls.append(new_tp)
        task_count += len(url_ls)
        for item in asin_ls:
            new_tp = tp.new_task({'platform': task_dct['platform'],
                                  'asin': item['asin'],
                                  'with_qty': task_dct.get('with_qty', False),
                                  'extra': {
                                      'bsr': {'bs_cate': [item['cate']], 'date': task_dct['date']}
                                      }
                                  })
            new_tp.set_to('output')
            task_ls.append(new_tp)
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
                "with_qty": True    #optional
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
        task_dct = tp.get_data()
        if task_dct['platform'] not in PLATFORM_FILTER_LS:
            return
        if task_start:
            tp.set_to('input_back')
            return tp
        else:
            group.suspend_endpoint('input')
            task_start = True
            logger.info(task_dct['root_url'])
            filter_ls = [cate.lower() for cate in task_dct['category_filter']]
            task_dct['url'] = task_dct['root_url']
            task_dct['date'] = time.strftime("%Y-%m-%d", time.localtime())
            del task_dct['root_url']
            del task_dct['category_filter']
            new_tp = tp.new_task(task_dct)
            new_tp.set_to('inner_output')
            task_count = 1
            return new_tp

    if from_name == 'notify' and task_start:
        if task.get_data() == b'task done':
            filter_ls = []
            task_start = False
            group.resume_endpoint('input')


def run():
    bsr_end = RabbitmqInputEndpoint('amz_bsr:input', **RABBITMQ_CONF)
    output_end = RabbitmqOutputEndpoint(['amz_bsr:input', 'amz_bsr:output',
                                         'amz_ip_ban:input'], **RABBITMQ_CONF)
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
    task_group.add_output_endpoint('input_back', output_end, 'amz_bsr:input')
    task_group.add_output_endpoint('inner_output', inner_output_end)

    worker_group = server.add_group('work', MAX_WORKERS)
    worker_group.set_handle(handle_worker)
    worker_group.add_input_endpoint('inner_input', inner_input_end)
    worker_group.add_output_endpoint('output', output_end, 'amz_bsr:output', buffer_size=MAX_WORKERS*20)
    worker_group.add_output_endpoint('inner_output', inner_output_end)
    worker_group.add_output_endpoint('notify', notify_output_end)
    worker_group.add_output_endpoint('ban', output_end, 'amz_ip_ban:input')
    server.run()
