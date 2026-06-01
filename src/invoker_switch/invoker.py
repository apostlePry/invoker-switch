"""SyncInvoker — 统一同步执行器

核心能力：在同步和异步代码之间提供透明的调用桥接。
"""

import asyncio
import concurrent.futures
import contextvars
import dis
import inspect
import sys
import threading
from abc import ABCMeta
from enum import StrEnum

from pydantic import BaseModel
from typing_extensions import Any, Callable, Dict, List, Optional


# ============================================================
# 辅助类型
# ============================================================


class MethodKind(StrEnum):
    """方法类型枚举"""

    SYNC = "sync"  # 普通 def 方法
    ASYNC = "async"  # async def 方法
    COROUTINE = "coroutine"  # 协程对象（已调用但未 await）


class CallFrame(BaseModel):
    """调用栈帧 — 记录当前方法调用信息，构成链表结构"""

    method_name: str  # 方法全限定名（如 UserService.get_user）
    method_kind: MethodKind  # 当前方法类型
    caller: Optional["CallFrame"] = None  # 调用者帧（链表指针）

    @property
    def caller_kind(self) -> Optional[MethodKind]:
        """获取调用者的方法类型"""
        if self.caller is None:
            return None
        return self.caller.method_kind


# ============================================================
# 模块级状态
# ============================================================

_call_stack: contextvars.ContextVar[List[CallFrame]] = contextvars.ContextVar(
    "_call_stack",
    default=[],
)

_instruction_cache: Dict[int, List[Any]] = {}


# ============================================================
# 字节码级 await 检测
# ============================================================


def _is_awaited() -> bool:
    """检查调用者是否使用了 await

    通过检查调用栈帧的字节码，判断调用指令后面是否紧跟 GET_AWAITABLE 指令。

    栈帧关系：
        调用方式 1 — 通过 RpcBase 子类方法：
          frame 3: user_code         ← result = await obj.method()
          frame 2: wrapper           ← _invoker.invoke(func, self, *args)
          frame 1: invoke            ← _is_awaited()
          frame 0: _is_awaited

        调用方式 2 — 直接调用 invoker.invoke：
          frame 2: user_code         ← result = await invoker.invoke(func)
          frame 1: invoke            ← _is_awaited()
          frame 0: _is_awaited
    """
    try:
        # frame 0: _is_awaited
        # frame 1: invoke
        frame = sys._getframe(1)

        # 检查 frame 2 是否是 RpcMeta 的 wrapper
        frame2 = sys._getframe(2)
        frame2_name = frame2.f_code.co_name

        if frame2_name == RpcMeta._WRAPPER_FUNC_NAME:
            # 通过 RpcBase 方法调用，实际调用者在 frame 3
            caller_frame = sys._getframe(3)
        else:
            # 直接调用 invoker.invoke，实际调用者在 frame 2
            caller_frame = frame2

        code = caller_frame.f_code
        lasti = caller_frame.f_lasti

        # 使用缓存的指令列表
        cache_key = id(code)
        if cache_key not in _instruction_cache:
            _instruction_cache[cache_key] = list(dis.get_instructions(code))
        instrs = _instruction_cache[cache_key]

        for instr in instrs:
            if instr.offset > lasti:
                if instr.opname == "GET_AWAITABLE":
                    return True
                break
    except Exception:
        pass
    return False


# ============================================================
# EventLoopManager
# ============================================================


class EventLoopManager:
    """事件循环管理器 — 双模式（外部注入 / 内置创建）"""

    # 外部注入模式的状态
    _external_loop: Optional[asyncio.AbstractEventLoop] = None
    _external_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

    # 内置模式的状态
    _internal_loop: Optional[asyncio.AbstractEventLoop] = None
    _internal_thread: Optional[threading.Thread] = None
    _internal_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    _started: threading.Event = threading.Event()
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def set_event_loop(
        cls,
        loop: asyncio.AbstractEventLoop,
        executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
    ) -> None:
        """注入外部事件循环（如 FastAPI 的）"""
        cls._external_loop = loop
        cls._external_executor = executor

    @classmethod
    def clear_event_loop(cls) -> None:
        """清除外部循环，切回内置模式"""
        cls._external_loop = None
        cls._external_executor = None

    @classmethod
    def get_event_loop(cls) -> asyncio.AbstractEventLoop:
        """获取事件循环：优先外部，否则内置"""
        if cls._external_loop is not None:
            return cls._external_loop
        return cls._ensure_internal_loop()

    @classmethod
    def get_executor(cls) -> concurrent.futures.ThreadPoolExecutor:
        """获取线程池：优先外部，否则内置"""
        if cls._external_executor is not None:
            return cls._external_executor
        return cls._ensure_internal_executor()

    @classmethod
    def _ensure_internal_loop(cls) -> asyncio.AbstractEventLoop:
        """双重检查锁定创建内置事件循环"""
        if cls._internal_loop is not None:
            if cls._internal_loop.is_closed():
                cls._internal_loop = None
                cls._internal_thread = None
            else:
                return cls._internal_loop

        with cls._lock:
            if cls._internal_loop is not None:
                if cls._internal_loop.is_closed():
                    cls._internal_loop = None
                    cls._internal_thread = None
                else:
                    return cls._internal_loop

            # 尝试获取当前运行的事件循环
            try:
                cls._internal_loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            # 没有运行中的循环，创建后台循环
            if cls._internal_loop is None:
                cls._started = threading.Event()
                cls._internal_loop = asyncio.new_event_loop()
                cls._internal_thread = threading.Thread(
                    target=cls._run_internal_loop,
                    daemon=True,
                    name="rpc-bg-loop",
                )
                cls._internal_thread.start()
                cls._started.wait()  # 等待循环启动
            return cls._internal_loop

    @classmethod
    def _run_internal_loop(cls) -> None:
        """在后台线程中运行内置事件循环"""
        loop = cls._internal_loop
        assert loop is not None
        asyncio.set_event_loop(loop)
        cls._started.set()  # 通知主线程循环已就绪
        loop.run_forever()

    @classmethod
    def _ensure_internal_executor(cls) -> concurrent.futures.ThreadPoolExecutor:
        """双重检查锁定创建内置线程池"""
        if cls._internal_executor is not None:
            return cls._internal_executor

        with cls._lock:
            if cls._internal_executor is not None:
                return cls._internal_executor

            cls._internal_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=10,
                thread_name_prefix="rpc-worker",
            )
            return cls._internal_executor

    @classmethod
    def shutdown(cls) -> None:
        """关闭内置资源"""
        if cls._internal_loop is not None:
            cls._internal_loop.call_soon_threadsafe(cls._internal_loop.stop)
            if cls._internal_thread is not None:
                cls._internal_thread.join(timeout=5)
            cls._internal_loop = None
            cls._internal_thread = None

        if cls._internal_executor is not None:
            cls._internal_executor.shutdown(wait=False)
            cls._internal_executor = None


# ============================================================
# SyncInvoker 核心
# ============================================================


class SyncInvoker:
    """统一同步执行器

    在同步和异步代码之间提供透明的调用桥接，让用户完全不需要
    关心同步/异步边界，框架自动处理执行策略。
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

        检查 __wrapped__：RpcMeta 包装后的方法保留了原始方法引用。
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
        """执行方法，自动判断类型并统一调用

        决策矩阵：
            同步上下文 + ASYNC  → _submit_coro()    [事件循环执行，阻塞等待]
            同步上下文 + SYNC   → _submit_sync()    [线程池执行，阻塞等待]
            异步上下文 + ASYNC  → _execute_async()   [返回协程]
            异步上下文 + SYNC + await  → _execute_sync_as_coro() [to_thread 包装]
            异步上下文 + SYNC + 无await → _execute_sync()        [直接执行]
        """
        # 1. 判断方法类型
        kind = self._get_method_kind(func)
        # 2. 获取调用者帧
        caller = self.current_frame
        # 3. 字节码检测 await
        is_awaited = _is_awaited()
        # 4. 判断执行上下文
        in_async = self._is_in_async_context()

        # ─── 同步上下文 ───
        if not in_async:
            if kind == MethodKind.ASYNC:
                # 异步方法 → 提交到事件循环，同步等待
                return self._submit_coro(func, args, kwargs, caller)
            else:
                # 同步方法 → 提交到线程池，同步等待
                return self._submit_sync(func, args, kwargs, caller)

        # ─── 异步上下文 ───
        if kind == MethodKind.ASYNC:
            # 异步方法 → 返回协程
            return self._execute_async(func, args, kwargs, caller)
        else:
            if is_awaited:
                # 同步方法 + await → 包装为协程（线程池执行）
                return self._execute_sync_as_coro(func, args, kwargs, caller)
            else:
                # 同步方法 + 无 await → 直接执行
                return self._execute_sync(func, args, kwargs, caller)


# ============================================================
# 全局实例 + 元类 + 基类
# ============================================================

_invoker: SyncInvoker = SyncInvoker()


class RpcMeta(ABCMeta):
    """元类：拦截类创建，包装所有方法

    _wrap_method 作为类方法而非模块级函数，好处：
    1. 职责内聚 —— 包装逻辑和使用它的元类在同一个类中
    2. 可覆写 —— RpcMeta 子类可以定制包装策略
    3. invoker 来源可扩展 —— 通过 _get_invoker() 获取，而非硬编码全局变量
    """

    # 供 _is_awaited() 检测用的 wrapper 函数名常量
    _WRAPPER_FUNC_NAME: str = "wrapper"

    @classmethod
    def _get_invoker(cls) -> SyncInvoker:
        """获取 invoker 实例，子类可覆写此方法来定制 invoker 来源"""
        return _invoker

    @classmethod
    def _wrap_method(cls, name: str, func: Callable[..., Any]) -> Callable[..., Any]:
        """包装方法，将调用转发给 SyncInvoker

        Args:
            name: 方法名
            func: 原始方法

        Returns:
            包装后的方法，调用时自动转发给 SyncInvoker.invoke()
        """
        invoker = cls._get_invoker()

        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            return invoker.invoke(func, self, *args, **kwargs)

        wrapper.__name__ = name
        wrapper.__qualname__ = func.__qualname__
        wrapper.__wrapped__ = func  # 保留原始方法引用
        return wrapper

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: Dict[str, Any],
    ) -> "RpcMeta":
        new_namespace: Dict[str, Any] = {}

        for attr_name, attr_value in namespace.items():
            # 跳过双下划线方法（__init__, __repr__ 等）
            if attr_name.startswith("__") and attr_name.endswith("__"):
                new_namespace[attr_name] = attr_value
                continue

            # 包装可调用的非类属性
            if callable(attr_value) and not isinstance(attr_value, type):
                wrapped = mcs._wrap_method(attr_name, attr_value)
                # 保留抽象方法标记
                if getattr(attr_value, "__isabstractmethod__", False):
                    wrapped.__isabstractmethod__ = True
                new_namespace[attr_name] = wrapped
            else:
                new_namespace[attr_name] = attr_value

        return super().__new__(mcs, name, bases, new_namespace)


class RpcBase(metaclass=RpcMeta):
    """RPC 基类 — 子类方法自动转发给 SyncInvoker"""

    @classmethod
    def get_invoker(cls) -> SyncInvoker:
        """获取全局 invoker 实例"""
        return _invoker


# ============================================================
# 轻量级异步执行工具
# ============================================================


async def run_callable(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """在异步上下文中统一执行同步/异步方法

    与 SyncInvoker 的区别：
    - SyncInvoker：完整决策引擎，处理所有上下文组合
    - run_callable：轻量工具，只在异步上下文中使用
    """
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return await asyncio.to_thread(func, *args, **kwargs)
