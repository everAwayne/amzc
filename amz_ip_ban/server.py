import json
import zlib
import redis
import socket
import asyncio
import aiohttp
import functools
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal
from pipeflow import RabbitmqInputEndpoint, TimeInputEndpoint, TimeOutputEndpoint
from config import RABBITMQ_CONF, IP_REDIS_CONF


MAX_WORKERS = 1
KEY_NAME = "proxy"
conf = {
    "socket_timeout": 30,
    "socket_connect_timeout": 15,
    "socket_keepalive": True,
    "socket_keepalive_options": {
        socket.TCP_KEEPIDLE: 600,
        socket.TCP_KEEPCNT: 3,
        socket.TCP_KEEPINTVL: 60,
    }
}
conf.update(IP_REDIS_CONF)
redis_client = redis.Redis(**conf)


def redis_execute(func):
    @functools.wraps(func)
    def redis_execute_wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except redis.ConnectionError as e:
                logger.error('Redis ConnectionError')
                redis_client.connection_pool.disconnect()
                continue
            except redis.TimeoutError as e:
                logger.error('Redis TimeoutError')
                redis_client.connection_pool.disconnect()
                continue
    return redis_execute_wrapper


def remove_proxy(group, task):
    """Handle proxy remove
    """
    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    logger.info("remove %s" % task_dct)
    if task_dct['proxy']:
        ret = redis_execute(redis_client.srem)(KEY_NAME, task_dct['proxy'])
        if ret:
            tp.set_to('output')
            return tp


def release_proxy(group, task):
    """Handle proxy release
    """
    tp = TaskProtocal(task)
    task_dct = tp.get_data()
    logger.info("release %s" % task_dct)
    if task_dct['proxy']:
        redis_execute(redis_client.sadd)(KEY_NAME, task_dct['proxy'])


def run():
    ban_input_end = RabbitmqInputEndpoint('amz_ip_ban:input', **RABBITMQ_CONF)
    release_ip_end = TimeInputEndpoint('amz_banned_ip', **IP_REDIS_CONF)
    ban_ip_end = TimeOutputEndpoint([('amz_banned_ip', 1440)], **IP_REDIS_CONF)

    server = pipeflow.Server()
    group = server.add_group('remove', 1)
    group.add_input_endpoint('input', ban_input_end)
    group.add_output_endpoint('output', ban_ip_end)
    group.set_handle(remove_proxy)

    group = server.add_group('realse', 1)
    group.add_input_endpoint('input', release_ip_end)
    group.set_handle(release_proxy)
    server.run()
