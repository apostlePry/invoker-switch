"""轻量级统一执行工具 — 同步/异步双模式，由用户显式选择"""

from typing_extensions import Any, Callable

from .meta import _invoker


async def arun_callable(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """异步模式执行同步/异步方法，始终返回协程

    在异步上下文中使用，需要 await：
        result = await arun_callable(sync_func)    → to_thread 卸载
        result = await arun_callable(async_func)   → 直接 await

    适用于：
        - async def 函数内部
        - asyncio.gather(*[arun_callable(f) for f in funcs])
        - FastAPI 路由等异步场景

    内部使用 _invoker.invoke() 作为执行引擎，决策逻辑与 SyncInvoker 一致。

    Args:
        func: 要执行的函数，可以是同步或异步
        *args: 位置参数
        **kwargs: 关键字参数
    """
    return await _invoker.invoke(func, *args, force_async=True, **kwargs)


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

    内部使用 _invoker.invoke() 作为执行引擎，决策逻辑与 SyncInvoker 一致。

    Args:
        func: 要执行的函数，可以是同步或异步
        *args: 位置参数
        **kwargs: 关键字参数

    Returns:
        同步模式：直接返回结果
        异步模式：返回协程（需 await）
    """
    return _invoker.invoke(func, *args, **kwargs)
