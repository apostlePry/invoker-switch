"""基础功能测试 — 六种执行场景"""

import asyncio

import pytest

from invoker_switch import InvokerBase, MethodKind, SyncInvoker, arun_callable, run_callable


# ─── 测试用 Service ───


class SimpleService(InvokerBase):
    """简单服务，用于基础场景测试"""

    def sync_hello(self) -> str:
        return "hello"

    async def async_hello(self) -> str:
        return "async_hello"

    def sync_add(self, a: int, b: int) -> int:
        return a + b

    async def async_add(self, a: int, b: int) -> int:
        return a + b


# ─── 场景 1：同步上下文 + 同步方法 ───


class TestSyncContextSyncMethod:
    """同步上下文 + 同步方法 → _submit_sync → 线程池执行，阻塞等待"""

    def test_sync_hello(self):
        svc = SimpleService()
        result = svc.sync_hello()
        assert result == "hello"

    def test_sync_add(self):
        svc = SimpleService()
        result = svc.sync_add(3, 5)
        assert result == 8


# ─── 场景 2：同步上下文 + 异步方法 ───


class TestSyncContextAsyncMethod:
    """同步上下文 + 异步方法 → _submit_coro → 事件循环执行，阻塞等待"""

    def test_async_hello(self):
        svc = SimpleService()
        result = svc.async_hello()
        assert result == "async_hello"

    def test_async_add(self):
        svc = SimpleService()
        result = svc.async_add(10, 20)
        assert result == 30


# ─── 场景 3 & 4：异步上下文 + 异步方法 ───


class TestAsyncContextAsyncMethod:
    """异步上下文 + 异步方法 → _execute_async → 返回协程"""

    async def test_async_hello_with_await(self):
        svc = SimpleService()
        result = await svc.async_hello()
        assert result == "async_hello"

    async def test_async_add_with_await(self):
        svc = SimpleService()
        result = await svc.async_add(7, 3)
        assert result == 10


# ─── 场景 5：异步上下文 + 同步方法 + await ───


class TestAsyncContextSyncMethodWithAwait:
    """异步上下文 + 同步方法 + await → _execute_sync_as_coro → to_thread"""

    async def test_sync_hello_with_await(self):
        svc = SimpleService()
        result = await svc.sync_hello()
        assert result == "hello"

    async def test_sync_add_with_await(self):
        svc = SimpleService()
        result = await svc.sync_add(4, 6)
        assert result == 10


# ─── 场景 6：异步上下文 + 同步方法 + 无 await ───


class TestAsyncContextSyncMethodNoAwait:
    """异步上下文 + 同步方法 + 无 await → _execute_sync → 直接执行"""

    async def test_sync_hello_no_await(self):
        svc = SimpleService()
        result = svc.sync_hello()
        assert result == "hello"

    async def test_sync_add_no_await(self):
        svc = SimpleService()
        result = svc.sync_add(1, 2)
        assert result == 3


# ─── run_callable 测试 ───


class TestRunCallableSyncContext:
    """同步上下文中使用 run_callable"""

    def test_sync_func_in_sync_context(self):
        """同步上下文 + 同步函数 → 线程池执行，直接返回结果"""
        def sync_func():
            return "sync_result"

        result = run_callable(sync_func)
        assert result == "sync_result"

    def test_async_func_in_sync_context(self):
        """同步上下文 + 异步函数 → 提交到事件循环，阻塞等待结果"""
        async def async_func():
            return "async_result"

        result = run_callable(async_func)
        assert result == "async_result"

    def test_sync_func_with_args_in_sync_context(self):
        def add(a, b):
            return a + b

        result = run_callable(add, 3, 7)
        assert result == 10

    def test_async_func_with_args_in_sync_context(self):
        async def multiply(a, b):
            return a * b

        result = run_callable(multiply, 4, 5)
        assert result == 20


class TestRunCallableAsyncContext:
    """异步上下文中使用 run_callable（通过 await 触发异步模式）"""

    async def test_sync_func_with_await(self):
        """await run_callable(sync_func) → to_thread"""
        def sync_func():
            return "sync_result"

        result = await run_callable(sync_func)
        assert result == "sync_result"

    async def test_async_func_with_await(self):
        """await run_callable(async_func) → 直接 await"""
        async def async_func():
            return "async_result"

        result = await run_callable(async_func)
        assert result == "async_result"


class TestArunCallable:
    """arun_callable 显式异步模式测试"""

    async def test_sync_func(self):
        """arun_callable(sync_func) → to_thread"""
        def sync_func():
            return "sync_result"

        result = await arun_callable(sync_func)
        assert result == "sync_result"

    async def test_async_func(self):
        """arun_callable(async_func) → 直接 await"""
        async def async_func():
            return "async_result"

        result = await arun_callable(async_func)
        assert result == "async_result"

    async def test_sync_func_with_args(self):
        def add(a, b):
            return a + b

        result = await arun_callable(add, 3, 7)
        assert result == 10

    async def test_async_func_with_args(self):
        async def multiply(a, b):
            return a * b

        result = await arun_callable(multiply, 4, 5)
        assert result == 20

    async def test_gather(self):
        """arun_callable 配合 asyncio.gather 使用"""
        async def async_func(x):
            return x * 2

        results = await asyncio.gather(
            arun_callable(async_func, 1),
            arun_callable(async_func, 2),
            arun_callable(async_func, 3),
        )
        assert results == [2, 4, 6]


# ─── InvokerBase 基础测试 ───


class TestInvokerBase:
    def test_get_invoker(self):
        invoker = SimpleService.get_invoker()
        assert isinstance(invoker, SyncInvoker)

    def test_method_wrapped(self):
        """确认 InvokerMeta 已包装方法，__wrapped__ 指向原始方法"""
        # sync_hello 应该被包装
        assert hasattr(SimpleService.sync_hello, "__wrapped__")
        # __wrapped__ 应该是原始函数
        original = SimpleService.sync_hello.__wrapped__
        assert asyncio.iscoroutinefunction(original) is False

    def test_async_method_wrapped(self):
        """确认 async 方法也被包装，__wrapped__ 指向原始 async 方法"""
        assert hasattr(SimpleService.async_hello, "__wrapped__")
        original = SimpleService.async_hello.__wrapped__
        assert asyncio.iscoroutinefunction(original) is True

    def test_dunder_methods_not_wrapped(self):
        """双下划线方法不应被包装"""
        # __init__ 不应被包装
        assert not hasattr(SimpleService.__init__, "__wrapped__")
