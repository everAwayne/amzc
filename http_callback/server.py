import json
import asyncio
import aiohttp
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal
from util.rabbitmq_endpoints import RabbitmqInputEndpoint, RabbitmqOutputEndpoint


MAX_WORKERS = 5
TIME_OUT = 7


async def handle_worker(group, task):
    """Handle callback task
    """
    tp = TaskProtocal(task)
    logger.info(tp.get_data())
    task_dct = tp.get_data()
    if 'extra' in task_dct and 'cb' in task_dct['extra']:
        url = task_dct['extra']['cb'].get('url')
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, timeout=TIME_OUT, json=task_dct) as resp:
                    html = await resp.read()
                    if resp.status != 200:
                        logger.error('[%d] %s' % (resp.status, url))
            except Exception as exc:
                logger.error('Request page fail : %s' % exc)


def run():
    input_end = RabbitmqInputEndpoint('http_callback:input', host='192.168.0.10', port=5672,
            virtualhost="/", heartbeat_interval=120, login='guest', password='guest')
    server = pipeflow.Server()
    group = server.add_group('main', MAX_WORKERS)
    group.add_input_endpoint('input', input_end)
    group.set_handle(handle_worker)
    server.run()
