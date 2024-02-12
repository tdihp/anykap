
# from unittest.mock import Mock, patch
from anykap.sync import *


def test_fission(hq, hqthread):
    testevent = threading.Event()
    class FooBarTask(Task):
        def __init__(self, name, foo):
            super().__init__(name)
            self.foo = foo

        def run_task(self):
            testevent.set()

    class FooBarFission(Fission):
        def filter(self, item):
            return item.get('foo') == 'bar'

        def mutator(self, item):
            return {"name": item['foo'], "foo": "bar"}

    hq.add_rule(FooBarFission(hq, FooBarTask))
    # t = threading.Thread(name='hq', target=hq.run)
    hqthread.start()
    hq.send_event({'foo': 'bar'})
    # we wait for a short time so hq get to process the item
    testevent.wait()
    mytask, = hq.tasks
    assert isinstance(mytask, FooBarTask)
    assert mytask.foo == 'bar'
    assert mytask.name == 'bar'
    mytask.join(1)
    assert not mytask.is_alive()