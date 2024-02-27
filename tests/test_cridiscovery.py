import pytest
import time
from unittest.mock import AsyncMock, call
from anykap import *


async def test_periodic_task(hq, hqtask):
    times = [0.02, 0.1, 0.3, 0.8, 0.01]
    output = []
    done = asyncio.Future()
    class MyPeriodicTask(PeriodicTask):
        def __init__(self,):
            super().__init__(cadence=0.2, timeout=0.5)

        async def run_once(self, hq):
            if times:
                v = times.pop(0)
                if v > 0.5:
                    # make sure properly canceled
                    with pytest.raises(asyncio.CancelledError):
                        await asyncio.sleep(v)
                else:
                    await asyncio.sleep(v)
                output.append(time.monotonic())
            else:
                done.set_result(True)

    hq.add_task(MyPeriodicTask())
    await done
    diff = [a - b for b, a in  zip(output[:-1], output[1:])]
    print(diff)
    assert diff == pytest.approx([0.28, 0.4, 0.5, 0.01], rel=0.2)


async def test_cridiscovery(hq, hqtask):
    results = []

    mock = AsyncMock()
    class CRIDummy(CRICtlData):
        OBJTYPE = 'dummy'
        LIST_KEY = 'dummy'
        INSPECT_KEY = 'inspectdummy'
        DATA_LIST_KEY = 'items'
        DATA_KEYS = {'id',}
        INSPECT_KEYS = {'info',}
        @classmethod
        def query_args(cls, namespace=None):
            args = []
            if namespace:
                args += ['--namespace', namespace]
            return args
        run_crictl = mock

    mytask = CRIDiscovery(CRIDummy, cadence=0.1, namespace='foobar')
    crictl_outputs = [
        {'items': [{'id': 'item1',}]},
        {'items': [{'id': 'item1',}]},
        {'items': [{'id': 'item1',}, {'id': 'item2'}]},
        {'items': [{'id': 'item2'}]},
        {'items': [{'id': 'item3'}]},
        # lambda: mytask.exit(),
    ]
    done = asyncio.Future()
    def myrule(event):
        if event['kind'] == 'discovery':
            results.append(event)
            if len(results) == 5:
                done.set_result(True)

    mock.side_effect = crictl_outputs
    hq.add_rule(myrule)
    hq.add_task(mytask)
    await asyncio.wait_for(done, timeout=1)
    assert mock.await_args_list == [call(['crictl', 'dummy', '-o', 'json', '--namespace', 'foobar'])] * 5
    def r(id, topic):
        return {'kind': 'discovery',
                'topic': topic,
                'objtype': 'dummy',
                'task': 'cridiscovery-0',
                'object': CRIDummy({'id': id})}
    assert results == [
        r('item1', 'new'),
        r('item2', 'new'),
        r('item1', 'lost'),
        r('item2', 'lost'),
        r('item3', 'new'),
    ]
