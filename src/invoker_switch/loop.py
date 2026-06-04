"""事件循环管理器 — 双模式（外部注入 / 内置创建）"""

import asyncio
import threading

from concurrent.futures import ThreadPoolExecutor
from typing_extensions import Optional

from .executor import AdaptiveExecutor


class EventLoopManager:
    """事件循环管理器 — 双模式（外部注入 / 内置创建）

    外部注入模式：由 FastAPI 等框架提供事件循环，通过 set_event_loop() 注入。
    内置创建模式：没有外部循环时，自动创建后台线程运行事件循环。

    只有一个 loop 属性，外部设置和内部创建共用。
    外部设置时，如果内部已有 loop 则不再覆盖；
    内部创建时，如果外部已注入则不再创建。

    线程池同样支持双模式：外部注入或内置创建。
    内置模式下，AdaptiveExecutor 作为默认 executor 绑定到事件循环，
    run_in_executor(None, ...) 自动使用 AdaptiveExecutor。
    """

    # 事件循环（外部注入或内置创建，取其一）
    _loop: Optional[asyncio.AbstractEventLoop] = None
    # 线程池（外部注入或内置创建，取其一）
    _executor: Optional[ThreadPoolExecutor] = None

    # 内置模式的后台线程
    _thread: Optional[threading.Thread] = None
    _started: threading.Event = threading.Event()
    _lock: threading.RLock = threading.RLock()

    @classmethod
    def set_event_loop(
        cls,
        loop: asyncio.AbstractEventLoop,
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> None:
        """注入外部事件循环（如 FastAPI 的）

        如果内部已有事件循环则不再覆盖。
        """
        if cls._loop is not None:
            return
        cls._loop = loop
        if executor is not None:
            cls._executor = executor

    @classmethod
    def clear_event_loop(cls) -> None:
        """清除事件循环和线程池，切回初始状态"""
        cls._loop = None
        cls._executor = None

    @classmethod
    def get_event_loop(cls) -> asyncio.AbstractEventLoop:
        """获取事件循环：已有则直接返回，否则内置创建"""
        if cls._loop is not None:
            return cls._loop
        return cls._ensure_internal_loop()

    @classmethod
    def get_executor(cls) -> ThreadPoolExecutor:
        """获取线程池：已有则直接返回，否则内置创建"""
        if cls._executor is not None:
            return cls._executor
        return cls._ensure_internal_executor()

    @classmethod
    def _check_loop_closed(cls) -> bool:
        """检查事件循环是否已关闭"""
        assert cls._loop is not None
        if cls._loop.is_closed():
            cls._loop = None
            cls._thread = None
            return True
        return False

    @classmethod
    def _ensure_internal_loop(cls) -> asyncio.AbstractEventLoop:
        """双重检查锁定创建内置事件循环，并绑定默认线程池"""
        if cls._loop is not None:
            if not cls._check_loop_closed():
                return cls._loop

        with cls._lock:
            if cls._loop is not None:
                if not cls._check_loop_closed():
                    return cls._loop

            # 尝试获取当前运行的事件循环
            try:
                cls._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            # 没有运行中的循环，创建后台循环
            if cls._loop is None:
                cls._started = threading.Event()
                cls._loop = asyncio.new_event_loop()

                # 创建 AdaptiveExecutor 并设为 loop 的默认 executor
                cls._executor = cls._ensure_internal_executor()
                cls._loop.set_default_executor(cls._executor)

                # 主线程也设置 event loop，使 asyncio.get_event_loop() 可用
                asyncio.set_event_loop(cls._loop)

                cls._thread = threading.Thread(
                    target=cls._run_internal_loop,
                    daemon=True,
                    name="invoker-bg-loop",
                )
                assert cls._thread
                cls._thread.start()
                cls._started.wait()  # 等待循环启动

            assert cls._loop is not None
            return cls._loop

    @classmethod
    def _run_internal_loop(cls) -> None:
        """在后台线程中运行内置事件循环"""
        loop = cls._loop
        assert loop is not None
        asyncio.set_event_loop(loop)
        cls._started.set()  # 通知主线程循环已就绪
        loop.run_forever()

    @classmethod
    def _ensure_internal_executor(cls) -> ThreadPoolExecutor:
        """双重检查锁定创建内置线程池"""
        if cls._executor is not None:
            return cls._executor

        with cls._lock:
            if cls._executor is None:
                cls._executor = AdaptiveExecutor()
            assert cls._executor
            return cls._executor

    @classmethod
    def shutdown(cls) -> None:
        """关闭内置资源"""
        if cls._loop is not None:
            cls._loop.call_soon_threadsafe(cls._loop.stop)
            if cls._thread is not None:
                cls._thread.join(timeout=5)
            cls._loop = None
            cls._thread = None

        if cls._executor is not None:
            cls._executor.shutdown(wait=False)
            cls._executor = None