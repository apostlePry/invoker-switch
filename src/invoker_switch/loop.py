"""事件循环管理器 — 双模式（外部注入 / 内置创建）"""

import asyncio
import concurrent.futures
import threading

from typing_extensions import Optional


class EventLoopManager:
    """事件循环管理器 — 双模式（外部注入 / 内置创建）

    外部注入模式：由 FastAPI 等框架提供事件循环，通过 set_event_loop() 注入。
    内置创建模式：没有外部循环时，自动创建后台线程运行事件循环。

    线程池同样支持双模式：外部注入或内置创建。
    """

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
                    name="invoker-bg-loop",
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
                thread_name_prefix="invoker-worker",
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
