"""SyncInvoker 核心逻辑测试"""

import asyncio
import contextvars
import time

import pytest

from invoker_switch import (
    CallFrame,
    InvokerBase,
    MethodKind,
    SyncInvoker,
)


# ─── 辅助类型与 Service ───


class InvokerTestService(InvokerBase):
    """用于测试 SyncInvoker 各种逻辑的服务"""

    def sync_identity(self, x: int) -> int:
        return x

    async def async_identity(self, x: int) -> int:
        return x

    def sync_slow(self) -> str:
        time.sleep(0.01)
        return "slow"

    async def async_slow(self) -> str:
        await asyncio.sleep(0.01)
        return "async_slow"


# ─── _get_method_kind 测试 ───


class TestGetMethodKind:
    def test_sync_function(self):
        invoker = SyncInvoker()
        def sync_func():
            pass
        assert invoker._get_method_kind(sync_func) == MethodKind.SYNC

    def test_async_function(self):
        invoker = SyncInvoker()
        async def async_func():
            pass
        assert invoker._get_method_kind(async_func) == MethodKind.ASYNC

    def test_coroutine_object(self):
        invoker = SyncInvoker()

        async def async_func():
            pass

        coro = async_func()
        try:
            assert invoker._get_method_kind(coro) == MethodKind.COROUTINE
        finally:
            coro.close()  # 避免警告

    def test_wrapped_async_function(self):
        """被 __wrapped__ 标记的 async 函数应被识别为 ASYNC"""
        invoker = SyncInvoker()

        def wrapper():
            pass

        async def original():
            pass

        wrapper.__wrapped__ = original
        assert invoker._get_method_kind(wrapper) == MethodKind.ASYNC

    def test_invoker_meta_wrapped_method(self):
        """InvokerMeta 包装后的方法，通过 __wrapped__ 应识别出原始类型"""
        # sync_identity 的 wrapper 本身是 sync，但 __wrapped__ 指向原始 sync
        assert InvokerTestService.sync_identity.__wrapped__ is not None
        invoker = SyncInvoker()
        # wrapper 是 sync 函数
        assert invoker._get_method_kind(InvokerTestService.sync_identity) == MethodKind.SYNC

        # async_identity 的 wrapper 本身是 sync，但 __wrapped__ 指向原始 async
        assert invoker._get_method_kind(InvokerTestService.async_identity) == MethodKind.ASYNC


# ─── 调用栈管理测试 ───


class TestCallStack:
    def test_frame_scope_basic(self):
        """_frame_scope 应正确维护调用栈"""
        invoker = SyncInvoker()

        def dummy():
            pass

        # 栈应为空
        assert invoker.current_frame is None

        # 进入帧作用域
        with invoker._frame_scope(dummy, MethodKind.SYNC, None):
            frame = invoker.current_frame
            assert frame is not None
            assert frame.method_name == dummy.__qualname__
            assert frame.method_kind == MethodKind.SYNC
            assert frame.caller is None

        # 离开作用域后栈应为空
        assert invoker.current_frame is None

    def test_frame_scope_exception_safety(self):
        """_frame_scope 在异常时也应正确弹出栈帧"""
        invoker = SyncInvoker()

        def dummy():
            pass

        try:
            with invoker._frame_scope(dummy, MethodKind.SYNC, None):
                assert invoker.current_frame is not None
                raise ValueError("test")
        except ValueError:
            pass

        assert invoker.current_frame is None

    def test_nested_frames(self):
        """嵌套的帧应构成链表结构"""
        invoker = SyncInvoker()

        def outer():
            pass

        def inner():
            pass

        with invoker._frame_scope(outer, MethodKind.SYNC, None):
            frame1 = invoker.current_frame
            assert frame1.method_kind == MethodKind.SYNC
            assert frame1.caller is None

            with invoker._frame_scope(inner, MethodKind.ASYNC, frame1):
                frame2 = invoker.current_frame
                assert frame2.method_kind == MethodKind.ASYNC
                assert frame2.caller is frame1
                assert frame2.caller_kind == MethodKind.SYNC

            # 内层弹出后应回到外层
            assert invoker.current_frame is frame1

        # 外层弹出后应为空
        assert invoker.current_frame is None


# ─── ContextVar 隔离测试 ───


class TestContextVarIsolation:
    async def test_context_var_not_leaking_between_tasks(self):
        """不同 asyncio Task 之间的调用栈应互相隔离"""

        test_var: contextvars.ContextVar[str] = contextvars.ContextVar("test_var")

        class ContextService(InvokerBase):
            async def set_and_get(self, val: str) -> str:
                test_var.set(val)
                # 给其他 task 一个执行机会
                await asyncio.sleep(0.01)
                return test_var.get()

        svc = ContextService()

        # 并发执行两个 task，各自设置不同的值
        results = await asyncio.gather(
            svc.set_and_get("A"),
            svc.set_and_get("B"),
        )
        # 两个 task 应各自得到自己设置的值（不是对方的值）
        # 注意：由于 ContextVar 的隔离性，这可能不总是按顺序
        assert set(results) == {"A", "B"}


# ─── invoke 直接调用测试 ───


class TestInvokeDirect:
    """直接通过 invoker.invoke() 调用的测试"""

    def test_invoke_sync_from_sync(self):
        """同步上下文直接调用同步函数"""
        invoker = SyncInvoker()

        def add(a, b):
            return a + b

        result = invoker.invoke(add, 3, 4)
        assert result == 7

    def test_invoke_async_from_sync(self):
        """同步上下文直接调用异步函数"""
        invoker = SyncInvoker()

        async def greet(name):
            return f"hello, {name}"

        result = invoker.invoke(greet, "world")
        assert result == "hello, world"

    async def test_invoke_sync_from_async_no_await(self):
        """异步上下文直接调用同步函数（无 await）"""
        invoker = SyncInvoker()

        def add(a, b):
            return a + b

        result = invoker.invoke(add, 1, 2)
        assert result == 3

    async def test_invoke_async_from_async_with_await(self):
        """异步上下文直接调用异步函数（有 await）"""
        invoker = SyncInvoker()

        async def greet(name):
            return f"hello, {name}"

        result = await invoker.invoke(greet, "world")
        assert result == "hello, world"
