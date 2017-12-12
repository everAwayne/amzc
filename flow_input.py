import sys
import json
import redis
import pika
import zlib
import random
import functools
import threading
from config import REDIS_CONF


PY3 = sys.version_info > (3,)
b_to_str = lambda x:x.decode('utf-8') if PY3 else x
str_to_b = lambda x:x.encode('utf-8') if PY3 else x


def redis_execute(func):
    @functools.wraps(func)
    def redis_execute_wrapper(*args, **kwargs):
        retry = 3
        while retry>0:
            retry -= 1
            try:
                return func(*args, **kwargs)
            except redis.ConnectionError as e:
                redis_client.connection_pool.disconnect()
            except redis.TimeoutError as e:
                redis_client.connection_pool.disconnect()
        else:
            raise RuntimeError("Fail to get conf")
    return redis_execute_wrapper


class FlowInput(object):
    _local = threading.local()
    _lock = threading.Lock()

    def __init__(self):
        FlowInput.load_conf()
        if getattr(self._local, "connection", None) is None:
            addr = random.choice(self.rabbitmq_conf_dct['addr'])
            credentials = pika.PlainCredentials(self.rabbitmq_conf_dct['authc']['account'],
                                                self.rabbitmq_conf_dct['authc']['password'])
            parameters = pika.ConnectionParameters(host=addr['ip'], port=addr['port'],
                    virtual_host="/", heartbeat_interval=0, credentials=credentials)
            self._local.parameters = parameters
            self._local.connection = pika.BlockingConnection(parameters)
            self._local.channel = self._local.connection.channel()
            self._local.channel.confirm_delivery()

    @staticmethod
    def load_conf():
        if not hasattr(FlowInput, "task_conf_dct") or not hasattr(FlowInput, "rabbitmq_conf_dct"):
            with FlowInput._lock:
                if not hasattr(FlowInput, "task_conf_dct") or not hasattr(FlowInput, "rabbitmq_conf_dct"):
                    redis_client = redis.Redis(**REDIS_CONF)
                    task_conf_dct = redis_execute(redis_client.hgetall)("task_conf")
                    FlowInput.task_conf_dct = dict([(b_to_str(k), json.loads(b_to_str(v)))
                                                    for k,v in task_conf_dct.items()])
                    rabbitmq_conf_dct = redis_execute(redis_client.hgetall)("rabbitmq_conf")
                    FlowInput.rabbitmq_conf_dct = dict([(b_to_str(k), json.loads(b_to_str(v)))
                                                        for k,v in rabbitmq_conf_dct.items()])
                    redis_client.connection_pool.disconnect()

    def send_task(self, task_type, task_data):
        """Send task data

        Success:
            return True
        Failure:
            return False
        """
        assert isinstance(task_data, dict), "task_data isn't a dictionary"
        assert task_type in self.task_conf_dct, "task_type isn't supported"
        queue_name = self.task_conf_dct[task_type][0]['name']
        task_dct = {
            "tid": task_type,
            "i": 0,
            "data": task_data
        }
        statistic_dct = {
            "tid": 'stats',
            "i": 0,
            "data": {
                'extra': {
                    'stats': {
                        'tid': task_type,
                        'step': 0
                    }
                }
            }
        }
        res = False
        retry = 3
        while retry > 0:
            retry -= 1
            try:
                res = self._local.channel.basic_publish(exchange='', routing_key=queue_name,
                        body=zlib.compress(str_to_b(json.dumps(task_dct))))
            except (pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed):
                print("[flow input]=====connection closed=====")
                self._local.connection.close()
                try:
                    self._local.connection = pika.BlockingConnection(self._local.parameters)
                    self._local.channel = connection.channel()
                    self._local.channel.confirm_delivery()
                except:
                    print("[flow input]=====reconnect error=====")
                    pass
            else:
                break
        if res:
            try:
                self._local.channel.basic_publish(exchange='', routing_key='statistic:input',
                                            body=zlib.compress(str_to_b(json.dumps(statistic_dct))))
            except (pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed):
                pass
        return res
