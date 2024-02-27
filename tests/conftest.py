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
def hq(tmp_path):
    return HQ(datapath=tmp_path / 'hq')


@pytest_asyncio.fixture
async def hqtask(hq):
    hqtask = asyncio.create_task(hq.run())
    yield hqtask
    hq.quit()
    await asyncio.wait_for(hqtask, timeout=5)


@pytest.fixture
def hqthread(hq, event_loop):
    """this one is for sync tests"""
    import traceback
    exception_raised = False
    def target():
        try:
            event_loop.run_until_complete(hq.run())
        except:
            nonlocal exception_raised
            exception_raised = True
            traceback.print_exc()

    hqthread = threading.Thread(target=target)
    yield hqthread
    hq.quit_threadsafe()
    hqthread.join(50)
    assert not hqthread.is_alive()
    assert not exception_raised
