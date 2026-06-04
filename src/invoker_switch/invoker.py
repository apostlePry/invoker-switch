"""SyncInvoker 核心执行器 — 在同步和异步代码之间提供透明的调用桥接"""

import asyncio
import contextvars
import functools
import inspect
from contextlib import contextmanager

from typing_extensions import Any, Callable, Dict, Optional, Tuple

from .detection import is_awaited
from .loop import EventLoopManager
from .types import CallFrame, MethodKind, _call_stack


async def _to_thread(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """asyncio.to_thread 的 Python 3.8 兼容实现

    asyncio.to_thread 在 Python 3.9 才引入。
    此实现复刻了 CPython 中 to_thread 的完整逻辑：
    1. contextvars.copy_context() 复制当前上下文
    2. ctx.run() 在新线程中以复制的上下文执行函数
    确保 ContextVar 在新线程中正确可见。
    """
    loop = EventLoopManager.get_event_loop()
    executor = EventLoopManager.get_executor()
    ctx = contextvars.copy_context()
    func_call = functools.partial(func, *args, **kwargs)
    return await loop.run_in_executor(executor, lambda: ctx.run(func_call))


class SyncInvoker:
    """统一同步执行器

    在同步和异步代码之间提供透明的调用桥接，让用户完全不需要
    关心同步/异步边界，框架自动处理执行策略。

    决策矩阵：
        ASYNC/COROUTINE 方法：
            异步调用链 → _run_async()     返回协程
            同步调用链 → _run_sync()      阻塞等待

        SYNC 方法：
            有 await   → _run_async()     返回协程（_dispatch → _to_thread）
            无 await   → _run_sync()      直接执行（快速路径）
    """

    # ─── 属性 ───

    @property
    def current_frame(self) -> Optional[CallFrame]:
        """获取当前调用栈顶帧"""
        stack = _call_stack.get()
        if not stack:
            return None
        return stack[-1]

    # ─── 上下文判断 ───

    @classmethod
    def _get_method_kind(cls, func: Callable[..., Any]) -> MethodKind:
        """判断方法类型

        检查 __wrapped__：InvokerMeta 包装后的方法保留了原始方法引用。
        如果不解包，所有方法都会被判断为 SYNC（因为 wrapper 本身是同步函数）。
        """
        original = getattr(func, "__wrapped__", func)
        target = original if inspect.iscoroutinefunction(original) else func

        if inspect.iscoroutinefunction(target):
            return MethodKind.ASYNC
        if inspect.iscoroutine(func):
            return MethodKind.COROUTINE
        return MethodKind.SYNC

    def _is_in_async_call_chain(self) -> bool:
        """是否在异步调用链中

        两个条件同时满足才返回 True：
        1. 有运行中的事件循环（物理上存在异步环境）
        2. 当前调用链的入口是异步方法（逻辑上属于异步调用链）

        为什么不能只用 get_running_loop()：
          _run_sync 会把协程提交到事件循环，事件循环线程里 get_running_loop()
          返回 True，但调用发起者是同步代码，应走同步路径。
          必须结合调用栈帧来判断真正的调用链归属。
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False

        caller = self.current_frame
        # caller 为 None：入口调用（在 async def main 中直接调用）
        # caller 为 ASYNC：当前方法被异步方法调用
        return caller is None or caller.method_kind == MethodKind.ASYNC

    # ─── 调用栈管理 ───

    @contextmanager
    def _frame_scope(
        self,
        func: Callable[..., Any],
        method_kind: MethodKind,
        caller: Optional[CallFrame],
    ):
        """调用栈帧的上下文管理器，确保 push/pop 总是成对执行"""
        frame = CallFrame(
            method_name=func.__qualname__,
            method_kind=method_kind,
            caller=caller,
        )
        stack = _call_stack.get().copy()
        stack.append(frame)
        token = _call_stack.set(stack)
        try:
            yield
        finally:
            _call_stack.reset(token)

    # ─── 统一分发 ───

    @classmethod
    async def _dispatch(cls, func: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
        """根据函数类型分发执行策略

        三种分发路径：
            协程对象  → 直接 await（已创建的协程，忽略 args/kwargs）
            async 函数 → await func(*args, **kwargs)（调用获得协程，再 await）
            sync 函数  → await _to_thread(func, *args, **kwargs)（卸载到线程池）
        """
        if inspect.iscoroutine(func):
            # 协程对象已经是"待执行的异步结果"，直接 await
            # 不能再传 args/kwargs——对协程对象调用 func(*args) 会创建新协程
            return await func
        if inspect.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return await _to_thread(func, *args, **kwargs)

    # ─── 异步上下文执行 ───

    async def _run_async(
        self,
        func: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """在异步上下文中运行函数，返回协程

        适用场景：异步调用链中的任何函数（async / sync / coroutine）
        async 函数 → 直接 await
        sync 函数  → _to_thread 卸载到线程池
        协程对象   → 直接 await

        帧类型根据 _get_method_kind 动态确定，而非硬编码为 ASYNC。
        """
        method_kind = self._get_method_kind(func)
        with self._frame_scope(func, method_kind, caller):
            return await self._dispatch(func, args, kwargs)

    # ─── 同步上下文执行 ───

    def _run_sync(
        self,
        func: Callable[..., Any],
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """在同步上下文中运行函数，阻塞等待结果

        sync 函数 → 直接执行并返回结果（快速路径）
        async 函数 / 协程对象 → 提交到事件循环，阻塞等待结果

        对于 async 函数，前提是当前线程不是事件循环线程。
        如果在事件循环线程中同步等待异步结果，会导致死锁。

        ContextVar 传播：
        run_coroutine_threadsafe 会在提交时的上下文中创建 Task，
        协程内部能正确读取当前线程的 ContextVar。
        """
        # sync 函数快速路径：直接执行，无需提交事件循环
        if not inspect.iscoroutinefunction(func) and not inspect.iscoroutine(func):
            with self._frame_scope(func, MethodKind.SYNC, caller):
                return func(*args, **kwargs)

        loop = EventLoopManager.get_event_loop()

        # 安全检查：当前线程是否是事件循环线程
        try:
            running_loop = asyncio.get_running_loop()
            in_loop_thread = running_loop is loop
        except RuntimeError:
            in_loop_thread = False

        if in_loop_thread:
            raise RuntimeError(
                f"Cannot block on async method '{func.__qualname__}' "
                f"inside the event loop thread. "
                f"Use 'await {func.__qualname__}()' instead."
            )

        future = asyncio.run_coroutine_threadsafe(
            self._run_async(func, args, kwargs, caller), loop
        )
        return future.result()

    # ─── 统一入口 ───

    def invoke(
        self,
        func: Callable[..., Any],
        *args: Any,
        force_async: bool = False,
        **kwargs: Any,
    ) -> Any:
        """执行方法，自动判断类型并统一调用

        决策逻辑：
            ASYNC/COROUTINE 方法：
                异步调用链 → 返回协程（_run_async）
                同步调用链 → 阻塞等待（_run_sync）
                判断依据：_is_in_async_call_chain() 或 force_async
                原因：ASYNC 方法返回协程，只有异步调用链才能 await 它；
                      同步调用链只能阻塞等待结果。

            SYNC 方法：
                有 await  → 返回协程（_run_async → _dispatch → _to_thread）
                无 await  → 直接执行（_run_sync 快速路径）
                判断依据：is_awaited() 或 force_async
                原因：SYNC 方法返回结果，用户通过 await 表达是否要异步执行。

        Args:
            func: 要执行的方法
            *args: 位置参数
            force_async: 强制异步模式，跳过 is_awaited() 和 _is_in_async_call_chain()
                检测，始终返回协程。适用于 arun_callable 等 async 函数内部调用
                的场景——此时用户帧已通过 GET_AWAITABLE，字节码检测无法生效。
            **kwargs: 关键字参数
        """
        # 1. 判断方法类型
        kind = self._get_method_kind(func)
        # 2. 获取调用者帧
        caller = self.current_frame

        # ─── ASYNC / COROUTINE 方法：能否返回协程？ ───
        if kind in (MethodKind.ASYNC, MethodKind.COROUTINE):
            if force_async or self._is_in_async_call_chain():
                # 异步调用链 → 可以返回协程
                return self._run_async(func, args, kwargs, caller)
            else:
                # 同步调用链 → 必须阻塞等待结果
                return self._run_sync(func, args, kwargs, caller)

        # ─── SYNC 方法：用户想同步还是异步执行？ ───
        if force_async or is_awaited():
            # 用户写了 await → 卸载到线程池，返回协程
            return self._run_async(func, args, kwargs, caller)
        else:
            # 用户没写 await → 同步上下文直接执行
            return self._run_sync(func, args, kwargs, caller)