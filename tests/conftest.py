import threading
import logging
import asyncio
import pytest
import pytest_asyncio

from anykap import HQ


@pytest.fixture(autouse=True)
def logconf(caplog):
    caplog.set_level(logging.DEBUG)


@pytest_asyncio.fixture
async def hq(tmp_path):
    return HQ(datapath=tmp_path / 'hq')
    # yield hq
    # await asyncio.wait_for(hqtask, timeout=5)  # 5 secs should be enough


@pytest_asyncio.fixture
async def hqtask(hq):
    hqtask = asyncio.create_task(hq.run())
    yield hqtask
    hq.quit.set_result(True)
    await asyncio.wait_for(hqtask, timeout=5)


# @pytest.fixture
# def hqthread(hq):
#     hqthread = threading.Thread(target=hq.run)
#     yield hqthread
#     if hq.running:
#         hq.running = False
#         # we expect hqthread can be joined reasonably fast
#         hqthread.join(timeout=5)

#     assert not hqthread.is_alive()
