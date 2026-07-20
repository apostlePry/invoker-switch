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
    def get_or_create_loop(cls) -> asyncio.AbstractEventLoop:
        """获取或创建事件循环，但不执行 run_forever

        使用 asyncio.new_event_loop() 或 asyncio.get_event_loop() 获取 loop，
        不启动后台线程，不执行 run_forever()。
        获取到的 loop 直接设置到 EventLoopManager._loop 中。
        """
        # 如果已经有保存的 loop，直接返回
        if cls._loop is not None:
            return cls._loop

        # 尝试获取当前线程的事件循环
        try:
            loop = asyncio.get_event_loop()
            if not loop.is_closed():
                cls._loop = loop  # 直接设置到 EventLoopManager
                return loop
        except RuntimeError:
            pass

        # 没有可用的事件循环，创建一个新的
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cls._loop = loop  # 直接设置到 EventLoopManager
        return loop

    @classmethod
    def get_event_loop(cls) -> asyncio.AbstractEventLoop:
        """获取事件循环：已有则直接返回，否则内置创建"""
        # 1. 如果已经有保存的 loop，直接返回
        if cls._loop is not None:
            return cls._loop

        # 2. 尝试获取当前正在运行的事件循环（FastAPI 等框架场景）
        try:
            running_loop = asyncio.get_running_loop()
            # 如果没有缓存，或者缓存的 loop 和当前运行的不是同一个，则覆盖
            if cls._loop is None or cls._loop is not running_loop:
                cls._loop = running_loop
            return cls._loop
        except RuntimeError:
            # 不在 async 上下文中，get_running_loop 会抛异常
            pass

        # 3. 不在 async 上下文中，使用 get_or_create_loop 获取/创建 loop
        loop = cls.get_or_create_loop()

        # 4. 判断 loop 是否正在运行
        if loop.is_running():
            # loop 已经在运行，直接返回
            return loop

        # 5. loop 没有运行，启动后台线程执行 run_forever()
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
        # 如果 loop 存在且正在运行，直接返回
        if cls._loop is not None and cls._loop.is_running():
            return cls._loop

        with cls._lock:
            # 再次检查，防止其他线程已经创建
            if cls._loop is not None and cls._loop.is_running():
                return cls._loop

            # 如果 loop 存在但没有运行，先关闭它
            if cls._loop is not None:
                if not cls._check_loop_closed():
                    # loop 存在且未关闭，但没有运行，需要重新创建
                    cls._loop = None

            # 创建新的事件循环
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
