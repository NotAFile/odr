import pytest

from odr.timeoutmgr import TimeoutManager


@pytest.fixture()
def timeout_mgr():
    """get a new TimeoutManager"""
    return TimeoutManager()
