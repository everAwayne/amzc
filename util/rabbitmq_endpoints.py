import aio_pika
import asyncio
from pipeflow.endpoints import AbstractCoroutineInputEndpoint, AbstractCoroutineOutputEndpoint
from pipeflow.tasks import Task
from util.log import logger


class RabbitMQClient:
    """Rabbitmq client"""

    def __init__(self, **conf):
        self._conf = {}
        self._conf.update(conf)


class RabbitmqInputEndpoint(RabbitMQClient, AbstractCoroutineInputEndpoint):
    """Rabbitmq aio input endpoint"""

    def __init__(self, queue_name, loop=None, **conf):
        self._loop = loop
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        self._inner_q = asyncio.Queue(1)
        super(RabbitmqInputEndpoint, self).__init__(**conf)
        self._loop.run_until_complete(self.initialize())

    async def initialize(self):
        while True:
            try:
                self._connection = await aio_pika.connect_robust(**self._conf)
                self._channel = await self._connection.channel()
                await self._channel.set_qos(prefetch_count=1)
                self._queue = await self._channel.declare_queue(self._queue_name, durable=True)
            except Exception as exc:
                logger.error("Connect error")
                logger.error(exc)
            else:
                await self._queue.consume(self._callback)
                break

    async def get(self):
        message = await self._inner_q.get()
        task = Task(message.body)
        return task

    async def _callback(self, message):
        await self._inner_q.put(message)
        message.ack()


class RabbitmqOutputEndpoint(RabbitMQClient, AbstractCoroutineOutputEndpoint):
    """Rabbitmq aio output endpoint"""

    def __init__(self, queue_name, persistent=False, loop=None, **conf):
        self._loop = loop
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        if queue_name is None:
            raise ValueError("queue_name must be not None")
        self._queue_name = queue_name
        self._persistent = persistent
        super(RabbitmqOutputEndpoint, self).__init__(**conf)
        self._loop.run_until_complete(self.initialize())

    async def initialize(self):
        while True:
            try:
                self._connection = await aio_pika.connect_robust(**self._conf)
                self._channel = await self._connection.channel()
                self._queue = await self._channel.declare_queue(self._queue_name, durable=True)
            except Exception as exc:
                logger.error("Connect error")
                logger.error(exc)
            else:
                break

    async def put(self, tasks):
        msgs = []
        for task in tasks:
            msgs.append(task.get_raw_data())
        await self._put(self._queue_name, msgs)
        return True

    async def _put(self, queue_name, msgs):
        """Put a message into a list
        """
        for msg in msgs:
            if self._persistent:
                message = aio_pika.Message(msg, delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
            else:
                message = aio_pika.Message(msg)
            await self._channel.default_exchange.publish(message, routing_key=self._queue_name)
