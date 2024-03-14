# import sys
# from inspect import getsource
# import zipfile
import pytest
from unittest.mock import Mock
import asyncio
import datetime
from anykap import Task, FissionRule, Filter, DelayRule


def _w(v):
    if v is None:
        return v
    elif isinstance(v, Exception):
        return Mock(side_effect=v)
    else:
        return Mock(return_value=v)


@pytest.mark.parametrize(
    "event,f,m,factory,called_with,raises,added",
    [
        ({"foo": "bar"}, True, "mutated", Task(), None, ValueError, False),
        (
            {"foo": "bar"},
            True,
            None,
            Task(),
            {"foo": "bar", "name": "foobar-1"},
            None,
            True,
        ),
        (
            {"foo": "bar"},
            True,
            {"foo1": "bar1", "name": "specified"},
            Task(),
            {"foo1": "bar1", "name": "specified"},
            None,
            True,
        ),
        (
            {"foo": "bar"},
            True,
            {"foo1": "bar1", "name": "specified"},
            object(),
            {"foo1": "bar1", "name": "specified"},
            ValueError,
            False,
        ),
    ],
)
def test_fission(event, f, m, factory, called_with, raises, added):
    hq = Mock()
    factory = _w(factory)
    rule = FissionRule(Filter(_w(f), _w(m)), hq, factory, name="foobar")
    if raises:
        with pytest.raises(raises):
            rule(event)
    else:
        rule(event)

    if called_with:
        called_with["context"] = {
            "task_created_by": "FissionRule",
            "rule_name": "foobar",
            "event": event,
        }
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
            hq.send_event({"foo": "bar"})
            await asyncio.sleep(0.3)
        done.set_result(True)

    results = []

    def outputrule(event):
        if event.get("kind") == "delay":
            results.append(event)

    rule = DelayRule(
        lambda x: x and x.get("foo") == "bar",
        hq,
        "foobar",
        1,
        throttle=datetime.timedelta(seconds=1),
    )
    hq.add_rule(outputrule)
    hq.add_rule(rule)
    await asyncio.wait_for(eventgen(), timeout=5)
    await asyncio.sleep(0.3)
    assert len(results) == 2
    for result in results:
        assert 3 <= result["count"] <= 4
        assert result["first_event"] == {"foo": "bar"}
        assert result["name"] == "foobar"
