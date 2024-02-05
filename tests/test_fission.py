
# from unittest.mock import Mock, patch
from anykap import *


def test_fission(caplog):
    caplog.set_level(logging.DEBUG)
    hq = HQ()

    class FooBarTask(Task):
        def __init__(self, name, foo):
            super().__init__(name)
            self.foo = foo

        def run_task(self):
            nonlocal hq
            hq.running = False
            time.sleep(1)

    class FooBarFission(Fission):
        def filter(self, item):
            return item.get('foo') == 'bar'

        def mutator(self, item):
            return {"name": item['foo'], "foo": "bar"}

    hq.add_rule(FooBarFission(hq, FooBarTask))
    t = threading.Thread(name='hq', target=hq.run)
    t.start()
    hq.send_event({'foo': 'bar'})
    t.join(3)
    assert not t.is_alive()
