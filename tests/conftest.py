"""测试公共 fixtures"""

import asyncio
import threading

import pytest

from invoker_switch import EventLoopManager


@pytest.fixture(autouse=True)
def cleanup_event_loop():
    """每个测试前后清理 EventLoopManager 状态，避免测试间干扰"""
    yield
    EventLoopManager.clear_event_loop()
    EventLoopManager.shutdown()
