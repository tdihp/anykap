import threading
import logging

import pytest

from anykap import HQ


@pytest.fixture(autouse=True)
def logconf(caplog):
    caplog.set_level(logging.DEBUG)


@pytest.fixture
def hq(tmp_path):
    return HQ(tmp_path / 'hq')


@pytest.fixture
def hqthread(hq):
    hqthread = threading.Thread(target=hq.run)
    yield hqthread
    if hq.running:
        hq.running = False
        # we expect hqthread can be joined reasonably fast
        hqthread.join(timeout=5)

    assert not hqthread.is_alive()
