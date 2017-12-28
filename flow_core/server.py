import json
import redis
import socket
import functools
import asyncio
import pipeflow
from util.log import logger
from util.task_protocal import TaskProtocal
from config import REDIS_CONF


FLOW_TASK_CONF = "task_conf"
FLOW_NODE_CONF = "node_conf"
MAX_WORKERS = 3
REFRESH_INTERVAL = 120

flow_conf = {}
conf = {
    "socket_timeout": 40,
    "socket_connect_timeout": 15,
    "socket_keepalive": True,
    "socket_keepalive_options": {
        socket.TCP_KEEPIDLE: 600,
        socket.TCP_KEEPCNT: 3,
        socket.TCP_KEEPINTVL: 60,
    }
}
conf.update(REDIS_CONF)
redis_client = redis.Redis(**conf)


async def handle_worker(group, task):
    tp = TaskProtocal(task)
    f = tp.get_from()
    tid = tp.get_tid()
    step = tp.get_step()

    logger.info("ep: %s, tid: %s, step: %s" % (f, tid, step))
    if tid not in flow_conf[FLOW_TASK_CONF]:
        logger.error("Task ID [%s] error" % tid)
        return
    task_ls = []
    task_data = tp.get_data()
    if step+1 < len(flow_conf[FLOW_TASK_CONF][tid]):
        endpoint_name = flow_conf[FLOW_TASK_CONF][tid][step+1]['name']
        next_tp = tp.new_task(task_data, next_step=True)
        next_tp.set_to(endpoint_name)
        task_ls.append(next_tp)
    for f_tid in flow_conf[FLOW_TASK_CONF][tid][step].get('fork', []):
        endpoint_name = flow_conf[FLOW_TASK_CONF][f_tid][0]['name']
        fork_tp = tp.new_task(task_data, tid=f_tid)
        fork_tp.set_to(endpoint_name)
        task_ls.append(fork_tp)
    return task_ls


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


def refresh_conf():
    global flow_conf
    task_conf_dct = redis_execute(redis_client.hgetall)(FLOW_TASK_CONF)
    task_conf_dct = dict([(k.decode('utf-8'), json.loads(v.decode('utf-8')))
                          for k,v in task_conf_dct.items()])
    node_conf_dct = redis_execute(redis_client.hgetall)(FLOW_NODE_CONF)
    node_conf_dct = dict([(k.decode('utf-8'), json.loads(v.decode('utf-8')))
                          for k,v in node_conf_dct.items()])
    node_fork_map = {}
    for v in node_conf_dct.values():
        if v.get('fork', []) and v.get('i', []):
            for i in v['i']:
                node_fork_map[i['queue']] = set(v['fork'])
    for v in task_conf_dct.values():
        for item in v:
            if item['name'] in node_fork_map:
                item['fork'] = set(item.get('fork',[])) | node_fork_map[item['name']]
    flow_conf[FLOW_TASK_CONF] = task_conf_dct
    flow_conf[FLOW_NODE_CONF] = node_conf_dct
    merge_ls = []
    merge_dct = {}
    for item in node_conf_dct.values():
        if 'i' in item:
            for conf in item['i']:
                queue = conf.pop('queue')
                if conf not in merge_ls:
                    merge_ls.append(conf)
                    index = len(merge_ls)-1
                    merge_dct[index] = []
                else:
                    index = merge_ls.index(conf)
                merge_dct[index].append(queue)
    flow_conf['merge_ls'] = merge_ls
    flow_conf['merge_dct'] = merge_dct


async def refresh_routine(server):
    while True:
        await asyncio.sleep(REFRESH_INTERVAL)
        refresh_conf()


def run():
    refresh_conf()
    server = pipeflow.Server()
    server.add_worker(refresh_routine)
    group = server.add_group('main', MAX_WORKERS)
    for i in range(len(flow_conf['merge_ls'])):
        conf = flow_conf['merge_ls'][i]
        queue_ls = flow_conf['merge_dct'][i]
        if conf['type'] == 'redis':
            ep = pipeflow.RedisOutputEndpoint(queue_ls, host=conf['host'],
                                              port=conf['port'], db=conf['db'],
                                              password=conf['password'])
            for queue in queue_ls:
                group.add_output_endpoint(queue, ep, queue)
        elif conf['type'] == 'rabbitmq':
            ep = pipeflow.RabbitmqOutputEndpoint(queue_ls, host=conf['host'],
                                        port=conf['port'], virtualhost=conf['virtualhost'],
                                        heartbeat_interval=conf['heartbeat'],
                                        login=conf['login'], password=conf['password'])
            for queue in queue_ls:
                group.add_output_endpoint(queue, ep, queue)
    for node in flow_conf[FLOW_NODE_CONF]:
        if 'o' in flow_conf[FLOW_NODE_CONF][node]:
            for conf in flow_conf[FLOW_NODE_CONF][node]['o']:
                if conf['type'] == 'redis':
                    ep = pipeflow.RedisInputEndpoint(conf['queue'], host=conf['host'],
                                                     port=conf['port'], db=conf['db'],
                                                     password=conf['password'])
                    group.add_input_endpoint(conf['queue'], ep)
                elif conf['type'] == 'rabbitmq':
                    ep = pipeflow.RabbitmqInputEndpoint(conf['queue'], no_ack=True, qos=MAX_WORKERS, host=conf['host'],
                                               port=conf['port'], virtualhost=conf['virtualhost'],
                                               heartbeat_interval=conf['heartbeat'],
                                               login=conf['login'], password=conf['password'])
                    group.add_input_endpoint(conf['queue'], ep)
    group.set_handle(handle_worker)
    server.run()
