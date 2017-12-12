import json
import zlib
import asyncio
import aiohttp
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal
from pipeflow import RabbitmqInputEndpoint, RabbitmqOutputEndpoint
from config import RABBITMQ_CONF


MAX_WORKERS = 15
TIME_OUT = 10


async def handle_worker(group, task):
    """Handle callback task
    """
    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    if 'extra' in task_dct and 'cb' in task_dct['extra']:
        url = task_dct['extra']['cb'].get('url')
        async with aiohttp.ClientSession(conn_timeout=7) as session:
            try:
                async with session.post(url, timeout=TIME_OUT, data=zlib.compress(json.dumps(task_dct).encode('utf-8'))) as resp:
                    html = await resp.read()
                    if resp.status != 200:
                        logger.error('[%d] %s' % (resp.status, url))
            except Exception as exc:
                logger.error('Request page fail : %s' % exc)


def run():
    input_end = RabbitmqInputEndpoint('http_callback:input', **RABBITMQ_CONF)
    server = pipeflow.Server()
    group = server.add_group('main', MAX_WORKERS)
    group.add_input_endpoint('input', input_end)
    group.set_handle(handle_worker)
    server.run()
