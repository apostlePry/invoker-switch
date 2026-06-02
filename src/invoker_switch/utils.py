"""轻量级统一执行工具 — 同步/异步上下文均可使用"""

import asyncio

from typing_extensions import Any, Callable, Union

from .loop import EventLoopManager


def _is_in_async_context() -> bool:
    """检查当前是否在异步上下文中（有运行中的事件循环）"""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def _run_sync_in_async(func: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
    """异步上下文中执行同步函数 — 通过 to_thread 卸载到线程池"""
    return asyncio.to_thread(func, *args, **kwargs)


def _run_async_in_sync(func: Callable[..., Any], args: tuple, kwargs: dict) -> Any:
    """同步上下文中执行异步函数 — 提交到事件循环，阻塞等待结果"""
    loop = EventLoopManager.get_event_loop()

    async def _wrapper():
        return await func(*args, **kwargs)

    future = asyncio.run_coroutine_threadsafe(_wrapper(), loop)
    return future.result()


def run_callable(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """统一执行同步/异步方法，自动适配当前上下文

    同步上下文：
        - 同步函数 → 线程池执行，阻塞等待结果
        - 异步函数 → 提交到事件循环，阻塞等待结果

    异步上下文：
        - 同步函数 → 通过 to_thread 卸载到线程池，返回协程
        - 异步函数 → 直接 await，返回协程

    与 SyncInvoker.invoke() 的区别：
        - run_callable：轻量工具，不做 await 检测、不维护调用栈、不处理重入死锁
        - SyncInvoker：完整决策引擎，处理所有上下文组合和边界情况

    适用于简单的函数式桥接场景，不需要声明 InvokerBase 子类。

    Args:
        func: 要执行的函数，可以是同步或异步
        *args: 位置参数
        **kwargs: 关键字参数

    Returns:
        异步上下文中返回协程（需 await），同步上下文中直接返回结果
    """
    is_async_func = asyncio.iscoroutinefunction(func)
    in_async = _is_in_async_context()

    # ─── 同步上下文 ───
    if not in_async:
        if is_async_func:
            # 异步函数 → 提交到事件循环，阻塞等待
            return _run_async_in_sync(func, args, kwargs)
        else:
            # 同步函数 → 线程池执行，阻塞等待
            executor = EventLoopManager.get_executor()
            future = executor.submit(func, *args, **kwargs)
            return future.result()

    # ─── 异步上下文 ───
    if is_async_func:
        # 异步函数 → 直接 await
        return func(*args, **kwargs)
    else:
        # 同步函数 → to_thread 卸载
        return _run_sync_in_async(func, args, kwargs)
