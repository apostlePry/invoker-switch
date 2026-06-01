"""外部事件循环注入测试 — 模拟 FastAPI 等框架场景"""

import asyncio
import threading

import pytest

from invoker_switch import EventLoopManager, RpcBase


class ExternalLoopService(RpcBase):
    """用于测试外部循环注入的服务"""

    def sync_work(self) -> str:
        return "sync_done"

    async def async_work(self) -> str:
        return "async_done"

    async def async_compute(self, x: int) -> int:
        return x * 3


def _run_loop_in_thread(loop: asyncio.AbstractEventLoop, started: threading.Event):
    """在后台线程中运行事件循环，模拟 FastAPI 的循环"""
    asyncio.set_event_loop(loop)
    started.set()
    loop.run_forever()


class TestExternalLoopInjection:
    """测试外部事件循环注入模式

    注意：外部循环必须在运行中才能被 _submit_coro 使用。
    FastAPI 等框架的循环天然在运行，测试中需要手动启动。
    """

    def test_sync_call_with_external_loop(self):
        """注入外部循环后，同步调用异步方法应正常工作"""
        loop = asyncio.new_event_loop()
        started = threading.Event()
        thread = threading.Thread(target=_run_loop_in_thread, args=(loop, started), daemon=True)
        thread.start()
        started.wait()
        try:
            EventLoopManager.set_event_loop(loop)
            svc = ExternalLoopService()
            result = svc.async_work()
            assert result == "async_done"
        finally:
            EventLoopManager.clear_event_loop()
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            loop.close()

    def test_mixed_calls_with_external_loop(self):
        """注入外部循环后，混合调用同步和异步方法"""
        loop = asyncio.new_event_loop()
        started = threading.Event()
        thread = threading.Thread(target=_run_loop_in_thread, args=(loop, started), daemon=True)
        thread.start()
        started.wait()
        try:
            EventLoopManager.set_event_loop(loop)
            svc = ExternalLoopService()

            assert svc.sync_work() == "sync_done"
            assert svc.async_work() == "async_done"
        finally:
            EventLoopManager.clear_event_loop()
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            loop.close()

    def test_async_call_within_running_loop(self):
        """在运行中的事件循环内部（模拟 FastAPI 路由），异步调用方法"""
        loop = asyncio.new_event_loop()
        started = threading.Event()
        thread = threading.Thread(target=_run_loop_in_thread, args=(loop, started), daemon=True)
        thread.start()
        started.wait()

        async def run():
            EventLoopManager.set_event_loop(asyncio.get_running_loop())
            svc = ExternalLoopService()
            result = await svc.async_compute(5)
            assert result == 15
            EventLoopManager.clear_event_loop()

        try:
            # 在外部循环中执行 async 测试
            future = asyncio.run_coroutine_threadsafe(run(), loop)
            result = future.result(timeout=10)
            assert result is None  # run() 返回 None，断言在内部已做
        finally:
            EventLoopManager.clear_event_loop()
            loop.call_soon_threadsafe(loop.stop)
            thread.join(timeout=5)
            loop.close()
