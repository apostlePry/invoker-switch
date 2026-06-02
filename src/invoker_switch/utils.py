"""轻量级统一执行工具 — 同步/异步双模式，由用户显式选择"""

import asyncio
import functools

from typing_extensions import Any, Callable, TypeVar

from .detection import _mark_wrapper, is_awaited
from .loop import EventLoopManager
from .meta import _invoker

T = TypeVar("T")


async def arun_callable(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """异步模式执行同步/异步方法，始终返回协程

    在异步上下文中使用，需要 await：
        result = await arun_callable(sync_func)    → to_thread 卸载
        result = await arun_callable(async_func)   → 直接 await

    适用于：
        - async def 函数内部
        - asyncio.gather(*[arun_callable(f) for f in funcs])
        - FastAPI 路由等异步场景

    Args:
        func: 要执行的函数，可以是同步或异步
        *args: 位置参数
        **kwargs: 关键字参数
    """
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return await asyncio.to_thread(func, *args, **kwargs)


def run_callable(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """统一执行同步/异步方法，由用户通过 await 或 arun_callable 控制执行模式

    执行模式由调用方式决定：

        无 await — 同步模式，直接返回结果：
            result = run_callable(sync_func)     → 直接调用
            result = run_callable(async_func)    → 提交到事件循环，阻塞等待

        有 await — 异步模式，返回协程：
            result = await run_callable(sync_func)   → to_thread 卸载
            result = await run_callable(async_func)  → 直接 await

    对于 gather 等需要显式协程的场景，使用 arun_callable：
        await asyncio.gather(*[arun_callable(f) for f in funcs])

    设计理念：
        是否在异步环境中应由用户把控，而不是框架自动检测事件循环。
        用户写 await 就是告诉框架"我在异步模式"，不写就是"我在同步模式"。

    与 SyncInvoker.invoke() 的区别：
        - run_callable：轻量函数式工具，不维护调用栈、不处理重入死锁
        - SyncInvoker：完整决策引擎，处理所有上下文组合和边界情况

    Args:
        func: 要执行的函数，可以是同步或异步
        *args: 位置参数
        **kwargs: 关键字参数

    Returns:
        同步模式：直接返回结果
        异步模式：返回协程（需 await）
    """
    is_async_func = asyncio.iscoroutinefunction(func)
    awaited = is_awaited()

    # ─── 同步模式（无 await）→ 直接返回结果 ───
    if not awaited:
        if is_async_func:
            # 异步函数 → 提交到事件循环，阻塞等待结果
            loop = EventLoopManager.get_event_loop()
            future = asyncio.run_coroutine_threadsafe(func(*args, **kwargs), loop)
            return future.result()
        else:
            # 同步函数 → 直接调用
            return func(*args, **kwargs)

    # ─── 异步模式（有 await）→ 返回协程 ───
    if is_async_func:
        # 异步函数 → 直接返回协程
        return func(*args, **kwargs)
    else:
        # 同步函数 → to_thread 卸载，返回协程
        return asyncio.to_thread(func, *args, **kwargs)


def smart_call(func: Callable[..., T]) -> Callable[..., T]:
    """装饰器：自动桥接同步/异步调用，让函数在任何上下文中都能正确执行

    用法：
        @smart_call
        def my_sync_func(x):
            return x * 2

        @smart_call
        async def my_async_func(x):
            return x * 2

    行为：
        同步调用链中调用 → 阻塞等待结果（async 函数自动提交到事件循环）
        异步调用链中调用 → 返回协程（sync 函数自动卸载到线程池）

    与 InvokerBase 的区别：
        - InvokerBase：通过元类自动包装所有方法，需要继承基类
        - smart_call：通过装饰器包装单个函数，无需继承，更灵活

    两者内部都使用 SyncInvoker.invoke() 作为执行引擎，决策逻辑完全一致。

    Args:
        func: 要包装的函数，可以是同步或异步

    Returns:
        包装后的函数，在任何上下文中都能正确执行
    """

    @functools.wraps(func)
    def _wrapper(*args: Any, **kwargs: Any) -> Any:
        return _invoker.invoke(func, *args, **kwargs)

    # 打上标记，让 _find_caller_frame 能识别并跳过此帧
    _mark_wrapper(_wrapper)
    return _wrapper
