"""SyncInvoker 核心执行器 — 在同步和异步代码之间提供透明的调用桥接"""

import asyncio
import contextvars
import inspect

from typing_extensions import Any, Callable, Dict, Optional

from .detection import is_awaited
from .loop import EventLoopManager
from .types import CallFrame, MethodKind, _call_stack


class SyncInvoker:
    """统一同步执行器

    在同步和异步代码之间提供透明的调用桥接，让用户完全不需要
    关心同步/异步边界，框架自动处理执行策略。

    决策矩阵：
        同步上下文 + ASYNC  → _submit_coro()         [事件循环执行，阻塞等待]
        同步上下文 + SYNC   → _submit_sync()         [线程池执行，阻塞等待]
        异步上下文 + ASYNC  → _execute_async()       [返回协程]
        异步上下文 + SYNC + await  → _execute_sync_as_coro() [to_thread 包装]
        异步上下文 + SYNC + 无await → _execute_sync()        [直接执行]
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

    def _get_method_kind(self, func: Callable[..., Any]) -> str:
        """判断方法类型

        检查 __wrapped__：InvokerMeta 包装后的方法保留了原始方法引用。
        如果不解包，所有方法都会被判断为 SYNC（因为 wrapper 本身是同步函数）。
        """
        original_func = getattr(func, "__wrapped__", func)

        if asyncio.iscoroutinefunction(original_func) or asyncio.iscoroutinefunction(func):
            return MethodKind.ASYNC

        if inspect.iscoroutine(func):
            return MethodKind.COROUTINE

        return MethodKind.SYNC

    def _is_in_async_context(self) -> bool:
        """检查是否在异步上下文中

        判断条件：有运行中的事件循环 且 调用者是异步方法（或为入口调用）。
        不能仅靠 get_running_loop() 判断，因为 SyncInvoker 在同步上下文中
        也会通过 _submit_coro 提交协程到事件循环。
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False  # 没有运行中的事件循环 → 同步上下文

        caller = self.current_frame
        # caller 为 None 表示入口调用（在 async def main 中直接调用）
        # caller 为 ASYNC 表示当前方法被异步方法调用
        return caller is None or caller.method_kind == MethodKind.ASYNC

    # ─── 调用栈管理 ───

    def _push_frame(
        self,
        func: Callable[..., Any],
        method_kind: MethodKind,
        caller: Optional[CallFrame],
    ) -> contextvars.Token:
        """压入调用栈帧，返回 token 用于恢复"""
        frame = CallFrame(
            method_name=func.__qualname__,
            method_kind=method_kind,
            caller=caller,
        )
        stack = _call_stack.get().copy()  # 复制，避免修改原列表
        stack.append(frame)
        return _call_stack.set(stack)  # 返回 token

    def _pop_frame(self, token: contextvars.Token) -> None:
        """弹出调用栈帧（恢复到 push 之前的状态）"""
        _call_stack.reset(token)

    # ─── 直接执行（异步上下文中使用） ───

    def _execute_sync(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """直接执行同步方法，返回结果

        适用场景：异步上下文 + 同步方法 + 无 await
        """
        token = self._push_frame(func, MethodKind.SYNC, caller)
        try:
            return func(*args, **kwargs)
        finally:
            self._pop_frame(token)

    async def _execute_sync_as_coro(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """将同步方法包装为协程执行（通过 asyncio.to_thread）

        适用场景：异步上下文 + 同步方法 + 有 await
        """
        ctx = contextvars.copy_context()

        def run_in_thread():
            return self._execute_sync(func, args, kwargs, caller)

        return await asyncio.to_thread(ctx.run, run_in_thread)

    async def _execute_async(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """await 执行异步方法

        适用场景：异步上下文 + 异步方法
        """
        token = self._push_frame(func, MethodKind.ASYNC, caller)
        try:
            return await func(*args, **kwargs)
        finally:
            self._pop_frame(token)

    # ─── 提交执行（同步上下文中使用） ───

    def _submit_sync(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """提交同步方法到线程池，同步等待结果

        适用场景：同步上下文 + 同步方法
        """
        executor = EventLoopManager.get_executor()
        future = executor.submit(
            self._execute_sync, func, args, kwargs, caller
        )
        return future.result()  # 阻塞等待

    def _submit_coro(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """提交协程到事件循环，同步等待结果（带重入死锁防护）

        适用场景：同步上下文 + 异步方法

        死锁场景：
          同步方法 a() 调用异步方法 b() → _submit_coro 提交 b 到事件循环
          → b() 在事件循环线程中执行
          → b() 调用同步方法 c() → _submit_coro 需要提交 c 到事件循环
          → 但事件循环线程正在执行 b，_submit_coro 的 future.result() 阻塞等待
          → 事件循环被 b 占用，无法执行 c → 死锁！

        死锁防护决策树：
          不在事件循环线程 → 安全阻塞等待
          在事件循环线程中 →
            有其他可用事件循环 → 提交到另一个循环
            没有其他循环 → 在线程池中创建临时新循环执行
        """
        loop = EventLoopManager.get_event_loop()
        ctx = contextvars.copy_context()

        # 检测是否在事件循环线程中
        try:
            running_loop = asyncio.get_running_loop()
            in_loop_thread = running_loop is loop
        except RuntimeError:
            in_loop_thread = False

        if in_loop_thread:
            # ─── 在事件循环线程中 → 不能阻塞等待当前循环 ───

            # 策略 1：尝试使用内置事件循环（如果存在且不是当前循环）
            internal_loop = EventLoopManager._internal_loop
            if internal_loop is not None and internal_loop is not running_loop:
                result, new_ctx = self._run_coro_with_context(
                    internal_loop, func, args, kwargs, caller, ctx
                )
                # 应用上下文变更到当前线程
                for var in new_ctx.keys():
                    var.set(new_ctx[var])
                return result

            # 策略 2：在线程池中创建新循环执行
            executor = EventLoopManager.get_executor()

            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(new_loop)
                    result, new_ctx = self._run_coro_with_context(
                        new_loop, func, args, kwargs, caller, ctx
                    )
                    return result, new_ctx
                finally:
                    new_loop.close()

            future = executor.submit(run_in_new_loop)
            result, new_ctx = future.result()
            for var in new_ctx.keys():
                var.set(new_ctx[var])
            return result

        # ─── 不在事件循环线程中 → 可以安全阻塞等待 ───
        result, new_ctx = self._run_coro_with_context(
            loop, func, args, kwargs, caller, ctx
        )
        for var in new_ctx.keys():
            var.set(new_ctx[var])
        return result

    # ─── 辅助方法 ───

    def _run_coro_with_context(
        self,
        loop: asyncio.AbstractEventLoop,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
        ctx: contextvars.Context,
    ) -> tuple[Any, contextvars.Context]:
        """在指定事件循环中运行协程并返回上下文

        核心问题：run_coroutine_threadsafe 在另一个线程执行协程，
        协程内部修改的 ContextVar 不会自动传播回调用线程。
        解决方案：协程执行后捕获新上下文，手动同步回调用线程。
        """
        result_holder: list[Any] = []
        ctx_holder: list[contextvars.Context] = []

        async def wrapper():
            result = await self._execute_async(func, args, kwargs, caller)
            # 捕获协程执行后的上下文
            ctx_holder.append(contextvars.copy_context())
            return result

        # 在复制的上下文中运行协程
        future = asyncio.run_coroutine_threadsafe(wrapper(), loop)
        result = future.result()  # 阻塞等待

        new_ctx = ctx_holder[0] if ctx_holder else ctx
        return result, new_ctx

    # ─── 统一入口 ───

    def invoke(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """执行方法，自动判断类型并统一调用"""
        # 1. 判断方法类型
        kind = self._get_method_kind(func)
        # 2. 获取调用者帧
        caller = self.current_frame
        # 3. 字节码检测 await
        awaited = is_awaited()
        # 4. 判断执行上下文
        in_async = self._is_in_async_context()

        # ─── 同步上下文 ───
        if not in_async:
            if kind == MethodKind.ASYNC:
                return self._submit_coro(func, args, kwargs, caller)
            else:
                return self._submit_sync(func, args, kwargs, caller)

        # ─── 异步上下文 ───
        if kind == MethodKind.ASYNC:
            return self._execute_async(func, args, kwargs, caller)
        else:
            if awaited:
                return self._execute_sync_as_coro(func, args, kwargs, caller)
            else:
                return self._execute_sync(func, args, kwargs, caller)
