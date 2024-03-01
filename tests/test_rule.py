# import sys
# from inspect import getsource
# import zipfile
import pytest
from unittest.mock import Mock, MagicMock
from anykap import *

def _w(v):
    if v is None:
        return v
    elif isinstance(v, Exception):
        return Mock(side_effect=v)
    else:
        return Mock(return_value=v)


@pytest.mark.parametrize('event,f,m,a,result,called,action_called,raises', [
    ({'foo': 'bar'}, True, None, None, {'foo': 'bar'}, True, False, None),
    ({'foo': 'bar'}, True, 'mutated', None, 'mutated', True, False, None),
    ({'foo': 'bar'}, True, 'mutated', 'OK', 'mutated', True, True, None),
    ({'foo': 'bar'}, False, None, None, None, False, False, None),
    ({'foo': 'bar'}, False, 'mutated', 'OK', None, False, False, None),
    ({'foo': 'bar'}, ValueError('oops'), 'mutated', 'OK', None, False, False, None),
    ({'foo': 'bar'}, True, ValueError('oops'), 'OK', None, False, False, None),
    ({'foo': 'bar'}, True, 'mutated', ValueError('oops'), 'mutated', True, True, ValueError),
])
def test_simplerule(event, f, m, a, result, called, action_called, raises):
    rule = SimpleRule(
        filter=_w(f),
        mutator=_w(m),
        action=_w(a))

    if raises:
        with pytest.raises(raises):
            assert rule(event) == result
    else:
        assert rule(event) == result
    assert rule.count == 1 if called else rule.count == 0
    if action_called:
        rule.action.assert_called_once()
    elif hasattr(rule, 'action'):
        rule.action.assert_not_called()


def test_compoundrule():
    class MyRule(CompoundRule):
        def myfilter(self, event):
            raise ValueError
    
    rule = MyRule()
    rule.add_rule(SimpleRule(rule.myfilter))
    assert rule({'foo': 'bar'}) == None


@pytest.mark.parametrize('event,f,m,factory,called_with,raises,added', [
    ({'foo': 'bar'}, True, 'mutated', Task(), None, ValueError, False),
    ({'foo': 'bar'}, True, None, Task(), {'foo': 'bar', 'name': 'foobar-1'}, None, True),
    ({'foo': 'bar'}, True, {'foo1': 'bar1', 'name': 'specified'}, Task(), {'foo1': 'bar1', 'name': 'specified'}, None, True),
    ({'foo': 'bar'}, True, {'foo1': 'bar1', 'name': 'specified'}, object(), {'foo1': 'bar1', 'name': 'specified'}, ValueError, False),
])
def test_fission(event, f, m, factory, called_with, raises, added):
    hq = Mock()
    factory = _w(factory)
    rule = FissionRule(
        hq, factory, name='foobar',
        filter=_w(f), mutator=_w(m),)
    if raises:
        with pytest.raises(raises):
            rule(event)
    else:
        rule(event)

    if called_with:
        factory.assert_called_once_with(**called_with)
    else:
        factory.assert_not_called()

    if added:
        hq.add_task.assert_called_once_with(factory.return_value)
    else:
        hq.add_task.assert_not_called()


async def test_delay(hq, hqtask):
    done = asyncio.Future()
    async def eventgen():
        for i in range(10):
            hq.send_event({'foo': 'bar'})
            await asyncio.sleep(0.3)
        done.set_result(True)
    results = []
    def outputrule(event):
        if event.get('kind') == 'delay':
            results.append(event)

    rule = DelayRule(hq, 'foobar', 1,
                     filter=lambda x: x.get('foo') == 'bar',
                     throttle=datetime.timedelta(seconds=1))
    hq.add_rule(outputrule)
    hq.add_rule(rule)
    eventtask = asyncio.create_task(eventgen())
    await asyncio.wait_for(done, timeout=5)
    await asyncio.sleep(0.3)
    assert len(results) == 2
    for result in results:
        assert 3 <= result['count'] <= 4
        assert result['first_event'] == {'foo': 'bar'}
        assert result['name'] == 'foobar'


async def test_future_receptor():
    receptor = FutureReceptor()
    wait_tasks = [asyncio.create_task(receptor.get()) for i in range(10)]
    await asyncio.sleep(0)  # allow get to be ran
    receptor.send({'foo1': 'bar'})
    done, pending = await asyncio.wait(wait_tasks, timeout=1)
    assert len(done) == 10
    # print('result:', done.pop().result())
    assert all((t.result() == {'foo1': 'bar'}) for t in done)
    receptor.add_rule(lambda event: event.get('foo2') == 'bar')
    wait_tasks = [asyncio.create_task(receptor.get()) for i in range(10)]
    await asyncio.sleep(0)  # allow get to be ran
    receptor({'foo2': 'bar'})
    done, pending = await asyncio.wait(wait_tasks, timeout=1)
    assert len(done) == 10
    assert all((t.result() is True) for t in done)
    wait_task = asyncio.create_task(receptor.get())
    await asyncio.sleep(0)  # allow get to be ran
    receptor({'foo3': 'bar'})
    with pytest.raises(asyncio.TimeoutError):
        result = await asyncio.wait_for(wait_task, timeout=0.1)
