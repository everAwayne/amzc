import json
import redis
import pika
import zlib
import random
import functools
import threading

CONF_HOST = "192.168.0.10"
CONF_PORT = 6379
CONF_DB = 10
CONF_PASSWD = None


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

    @staticmethod
    def load_conf():
        if not hasattr(FlowInput, "task_conf_dct") or not hasattr(FlowInput, "rabbitmq_conf_dct"):
            with FlowInput._lock:
                if not hasattr(FlowInput, "task_conf_dct") or not hasattr(FlowInput, "rabbitmq_conf_dct"):
                    redis_client = redis.Redis(host=CONF_HOST, port=CONF_PORT, db=CONF_DB, password=CONF_PASSWD)
                    task_conf_dct = redis_execute(redis_client.hgetall)("task_conf")
                    FlowInput.task_conf_dct = dict([(k, json.loads(v)) for k,v in task_conf_dct.items()])
                    rabbitmq_conf_dct = redis_execute(redis_client.hgetall)("rabbitmq_conf")
                    FlowInput.rabbitmq_conf_dct = {}
                    FlowInput.rabbitmq_conf_dct['addr'] = json.loads(rabbitmq_conf_dct['addr'])
                    FlowInput.rabbitmq_conf_dct['authc'] = json.loads(rabbitmq_conf_dct['authc'])
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
        queue_name = self.task_conf_dct[task_type][0]
        dct = {
            "tid": task_type,
            "i": 0,
            "data": task_data
        }
        retry = 4
        while retry > 0:
            retry -= 1
            try:
                return self._local.channel.basic_publish(exchange='', routing_key=queue_name,
                                            body=zlib.compress(json.dumps(dct)))
            except (pika.exceptions.ChannelClosed, pika.exceptions.ConnectionClosed):
                print "[flow input]=====connection closed====="
                self._local.connection.close()
                try:
                    self._local.connection = pika.BlockingConnection(self._local.parameters)
                    self._local.channel = connection.channel()
                except:
                    print "[flow input]=====reconnect error====="
                    pass
        return False
