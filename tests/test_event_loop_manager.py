"""EventLoopManager 测试"""

import asyncio
import threading

import pytest

from invoker_switch import EventLoopManager


class TestEventLoopManager:
    def test_get_event_loop_creates_internal(self):
        """没有外部循环时，应创建内置循环"""
        EventLoopManager.clear_event_loop()
        loop = EventLoopManager.get_event_loop()
        assert loop is not None
        assert not loop.is_closed()

    def test_set_and_get_external_loop(self):
        """设置外部循环后，get_event_loop 应返回外部循环"""
        new_loop = asyncio.new_event_loop()
        try:
            EventLoopManager.set_event_loop(new_loop)
            result = EventLoopManager.get_event_loop()
            assert result is new_loop
        finally:
            EventLoopManager.clear_event_loop()
            new_loop.close()

    def test_clear_event_loop(self):
        """clear_event_loop 后应回退到内置循环"""
        new_loop = asyncio.new_event_loop()
        try:
            EventLoopManager.set_event_loop(new_loop)
            EventLoopManager.clear_event_loop()
            # 清除后应创建新的内置循环
            loop = EventLoopManager.get_event_loop()
            assert loop is not new_loop
        finally:
            new_loop.close()

    def test_get_executor(self):
        """应返回有效的线程池"""
        executor = EventLoopManager.get_executor()
        assert executor is not None

    def test_set_external_executor(self):
        """设置外部线程池后，get_executor 应返回外部线程池"""
        from concurrent.futures import ThreadPoolExecutor

        ext_executor = ThreadPoolExecutor(max_workers=2)
        try:
            new_loop = asyncio.new_event_loop()
            EventLoopManager.set_event_loop(new_loop, ext_executor)
            result = EventLoopManager.get_executor()
            assert result is ext_executor
        finally:
            EventLoopManager.clear_event_loop()
            ext_executor.shutdown(wait=False)
            new_loop.close()

    def test_internal_loop_runs_in_background_thread(self):
        """内置循环应在后台线程中运行"""
        EventLoopManager.clear_event_loop()
        loop = EventLoopManager.get_event_loop()
        assert loop.is_running()
