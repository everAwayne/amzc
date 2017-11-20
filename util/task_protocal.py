import json
import zlib
import copy
from pipeflow import Task


class TaskProtocal(Task):
    __slots__ = ['_info']
    def __init__(self, task):
        assert isinstance(task, (Task, dict)), "task should be a instance of Task or dict"
        if isinstance(task, Task):
            _data = task.get_raw_data()
            super(TaskProtocal, self).__init__(_data)
            self.set_from(task.get_from())
            self.set_to(task.get_to())
            self.set_confirm_handle(task.get_confirm_handle())
            self._info = json.loads(zlib.decompress(_data).decode('utf-8'))
        else:
            super(TaskProtocal, self).__init__(zlib.compress(json.dumps(task).encode('utf-8')))
            self._info = task
        assert 'i' in self._info and 'tid' in self._info, "Task Protocal illegal"

    def get_data(self):
        return copy.deepcopy(self._info['data'])

    def get_tid(self):
        return self._info['tid']

    def get_step(self):
        return self._info['i']

    def new_task(self, data, tid=None, next_step=False):
        assert isinstance(data, dict), "data isn't a dict"
        if 'extra' in self._info['data']:
            dct = data.setdefault("extra", {})
            dct.update(self._info['data']['extra'])
        dct = {
            'tid': tid if tid else self._info['tid'],
            'i': 0 if tid else self._info['i']+1 if next_step else self._info['i'],
            'data': data
        }
        tp = TaskProtocal(dct)
        tp.set_confirm_handle(self.get_confirm_handle())
        return tp
