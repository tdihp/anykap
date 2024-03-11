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
    assert diff == pytest.approx([0.28, 0.4, 0.5, 0.01], abs=0.05)


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

    mytask = CRIDiscovery(CRIDummy, cadence=0.1, query=dict(namespace='foobar'))
    test_ladder = [
        (
            {'items': [{'id': 'item1',}]},
            [
                {'topic': 'first_seen'},
                {'topic': 'new', 'object': CRIDummy({'id': 'item1'})},
            ],
        ),
        (
            {'items': [{'id': 'item1'}, {'id': 'item2'}]},
            [{'topic': 'new', 'object': CRIDummy({'id': 'item2'})}],
        ),
        (
            {'items': [{'id': 'item2'}]},
            [{'topic': 'lost', 'object': CRIDummy({'id': 'item1'})}],
        ),
        (
            {'items': [{'id': 'item3'}]},
            [
                {'topic': 'lost', 'object': CRIDummy({'id': 'item2'})},
                {'topic': 'new', 'object': CRIDummy({'id': 'item3'})},
            ],
        ),
        (
            {'items': []},
            [
                {'topic': 'last_gone'},
                {'topic': 'lost', 'object': CRIDummy({'id': 'item3'})},
            ],
        ),
    ]
    done = asyncio.Future()
    results = [[] for _ in test_ladder]
    result_id = 0
    def side_effect():
        for items, result in test_ladder:
            yield items
            nonlocal result_id
            result_id += 1
        done.set_result(True)
        while True:
            yield {'items': []}

    def myrule(event):
        if event['kind'] == 'discovery':
            nonlocal result_id
            results[result_id].append(event)

    mock.side_effect = side_effect()
    hq.add_rule(myrule)
    hq.add_task(mytask)
    await asyncio.wait_for(done, timeout=1)
    assert mock.await_args_list == [call(['crictl', 'dummy', '-o', 'json', '--namespace', 'foobar'])] * 6
    event_base = {'objtype': 'dummy', 'task_name': 'cridiscovery-0', 'context': None, 'kind': 'discovery'}
    for (item, expected), actual in zip(test_ladder, results):
        assert [event_base | e for e in expected] == actual
