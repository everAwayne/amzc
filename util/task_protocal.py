import json
import zlib
from pipeflow import Task


class TaskProtocal:
    def __init__(self, task):
        assert isinstance(task, Task), "Task type error"
        self._task = task
        self._data = json.loads(zlib.decompress(self._task.get_data()).decode('utf-8'))
        assert 'i' in self._data and 'tid' in self._data, "Task Protocal illegal"

    def get_data(self):
        return self._data['data']

    def get_from(self):
        return self._task.get_from()

    def set_to(self, name):
        return self._task.set_to(name)

    def get_tid(self):
        return self._data['tid']

    def get_step(self):
        return self._data['i']

    def new_task(self, data, next_step=False):
        dct = {
            'tid': self._data['tid'],
            'i': self._data['i']+1 if next_step else self._data['i'],
            'data': data
        }
        return TaskProtocal(Task(zlib.compress(json.dumps(dct).encode('utf-8'))))

    def to_task(self):
        return self._task
