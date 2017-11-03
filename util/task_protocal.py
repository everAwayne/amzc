import json
import zlib
import copy
from pipeflow import Task


class TaskProtocal(Task):
    __slots__ = ['_info']
    def __init__(self, task):
        assert isinstance(task, (Task, dict)), "task should be a instance of Task or dict"
        if isinstance(task, Task):
            self._from = task.get_from()
            self._to = task.get_to()
            self._data = task.get_raw_data()
            self._info = json.loads(zlib.decompress(task.get_raw_data()).decode('utf-8'))
        else:
            self._data = zlib.compress(json.dumps(task).encode('utf-8'))
            self._info = task
        assert 'i' in self._info and 'tid' in self._info, "Task Protocal illegal"

    def get_data(self):
        return copy.deepcopy(self._info['data'])

    def get_tid(self):
        return self._info['tid']

    def get_step(self):
        return self._info['i']

    def new_task(self, data, next_step=False):
        assert isinstance(data, dict), "data isn't a dict"
        if 'extra' in self._info['data']:
            dct = data.setdefault("extra", {})
            dct.update(self._info['data']['extra'])
        dct = {
            'tid': self._info['tid'],
            'i': self._info['i']+1 if next_step else self._info['i'],
            'data': data
        }
        return TaskProtocal(dct)
