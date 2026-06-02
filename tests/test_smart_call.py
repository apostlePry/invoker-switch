"""smart_call 装饰器测试"""

import asyncio

import pytest

from invoker_switch import InvokerBase, SyncInvoker, smart_call


# ─── 装饰 sync 函数 ───


@smart_call
def sync_hello() -> str:
    return "hello"


@smart_call
def sync_add(a: int, b: int) -> int:
    return a + b


# ─── 装饰 async 函数 ───


@smart_call
async def async_hello() -> str:
    return "async_hello"


@smart_call
async def async_add(a: int, b: int) -> int:
    return a + b


# ─── 装饰类方法 ───


class MyService:
    @smart_call
    def sync_method(self) -> str:
        return "svc_sync"

    @smart_call
    async def async_method(self) -> str:
        return "svc_async"


# ─── 同步环境 ───


class TestSmartCallSyncContext:
    """同步环境中调用 smart_call 装饰的函数"""

    def test_sync_func(self):
        assert sync_hello() == "hello"

    def test_sync_func_with_args(self):
        assert sync_add(3, 5) == 8

    def test_async_func(self):
        """async 函数在同步环境中应阻塞等待结果"""
        assert async_hello() == "async_hello"

    def test_async_func_with_args(self):
        assert async_add(10, 20) == 30


# ─── 异步环境 ───


class TestSmartCallAsyncContext:
    """异步环境中调用 smart_call 装饰的函数"""

    async def test_sync_func_with_await(self):
        """sync 函数 + await → to_thread"""
        result = await sync_hello()
        assert result == "hello"

    async def test_sync_func_no_await(self):
        """sync 函数 + 无 await → 直接执行"""
        result = sync_hello()
        assert result == "hello"

    async def test_async_func_with_await(self):
        """async 函数 + await → 返回协程"""
        result = await async_hello()
        assert result == "async_hello"

    async def test_async_func_no_await(self):
        """async 函数 + 无 await → 返回协程"""
        result = async_hello()
        # 在异步调用链中，async 函数无论有没有 await 都应返回协程
        assert asyncio.iscoroutine(result)
        await result


# ─── 类方法 ───


class TestSmartCallClassMethod:
    """smart_call 装饰类方法"""

    def test_sync_method_in_sync_context(self):
        svc = MyService()
        assert svc.sync_method() == "svc_sync"

    def test_async_method_in_sync_context(self):
        svc = MyService()
        assert svc.async_method() == "svc_async"

    async def test_sync_method_in_async_context(self):
        svc = MyService()
        result = await svc.sync_method()
        assert result == "svc_sync"

    async def test_async_method_in_async_context(self):
        svc = MyService()
        result = await svc.async_method()
        assert result == "svc_async"


# ─── 与 InvokerBase 混合使用 ───


class InvokerService(InvokerBase):
    def invoker_method(self) -> str:
        return "from_invoker_base"


@smart_call
def call_invoker_service_method(svc: InvokerService) -> str:
    """smart_call 装饰的函数内部调用 InvokerBase 方法"""
    return svc.invoker_method()


class TestSmartCallWithInvokerBase:
    """smart_call 和 InvokerBase 混合使用"""

    def test_sync_context(self):
        svc = InvokerService()
        assert call_invoker_service_method(svc) == "from_invoker_base"

    async def test_async_context(self):
        svc = InvokerService()
        result = await call_invoker_service_method(svc)
        assert result == "from_invoker_base"


# ─── functools.wraps 兼容性 ───


class TestSmartCallWraps:
    """验证 smart_call 保留原始函数的元信息"""

    def test_preserves_name(self):
        @smart_call
        def my_function():
            pass

        assert my_function.__name__ == "my_function"

    def test_preserves_docstring(self):
        @smart_call
        def my_function():
            """This is my docstring"""
            pass

        assert my_function.__doc__ == "This is my docstring"

    def test_preserves_module(self):
        @smart_call
        def my_function():
            pass

        assert my_function.__module__ == __name__


# ─── wrapper 标记 ───


class TestSmartCallWrapperMark:
    """验证 smart_call 的 wrapper 被正确标记"""

    def test_wrapper_has_marker(self):
        from invoker_switch.detection import _WRAPPER_MARKER

        @smart_call
        def my_func():
            pass

        assert getattr(my_func, _WRAPPER_MARKER, False) is True
