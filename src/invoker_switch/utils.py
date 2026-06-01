"""轻量级异步执行工具"""

import asyncio

from typing_extensions import Any, Callable


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
