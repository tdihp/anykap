import threading
import logging
import asyncio
import pytest
import pytest_asyncio

from anykap import HQ


@pytest.fixture(autouse=True)
def logconf(caplog):
    caplog.set_level(logging.DEBUG)


@pytest.fixture
def hq(tmp_path, event_loop):
    return HQ(datapath=tmp_path / 'hq', loop=event_loop)
    # yield hq
    # await asyncio.wait_for(hqtask, timeout=5)  # 5 secs should be enough


@pytest_asyncio.fixture
async def hqtask(hq):
    hqtask = asyncio.create_task(hq.run())
    yield hqtask
    hq.quit.set_result(True)
    await asyncio.wait_for(hqtask, timeout=5)


@pytest.fixture
def hqthread(hq, event_loop):
    """this one is for sync tests"""
    hqthread = threading.Thread(target=event_loop.run_until_complete, args=[hq.run()])
    # hqthread.start()
    # test should call hqthread.start(), as add_task is not thread safe
    yield hqthread
    # hq.quit.set_result(True)
    event_loop.call_soon_threadsafe(hq.quit.set_result, True)
    hqthread.join(3)
    assert not hqthread.is_alive()
