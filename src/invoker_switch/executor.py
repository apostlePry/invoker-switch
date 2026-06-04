"""自适应线程池 — Java ThreadPoolExecutor 模型

核心线程始终存活，临时线程空闲超时后自动回收。
有界队列 + 拒绝策略，防止突发负载导致 OOM。
继承 ThreadPoolExecutor，复用 Future 管理逻辑，
只替换线程创建策略、队列类型和拒绝策略。
"""

import queue
import threading
import time
import weakref
from dataclasses import dataclass

from concurrent.futures import ThreadPoolExecutor, _base

from typing_extensions import Any, Callable, Dict, List, Optional


# ─── 模块级全局状态 ───
# 与 concurrent.futures.thread 保持一致，用于 interpreter shutdown 通知
_threads_queues: Dict[threading.Thread, queue.SimpleQueue] = weakref.WeakKeyDictionary()
_shutdown: bool = False
_global_shutdown_lock: threading.Lock = threading.Lock()


# ─── 监控指标数据类 ───

@dataclass(frozen=True)
class ExecutorStats:
    """线程池运行状态快照

    Attributes:
        active_threads:   当前活跃线程数（核心+临时）
        core_workers:     核心线程数配置
        max_workers:      最大线程数配置
        pending_tasks:    队列中等待执行的任务数
        keep_alive:       临时线程空闲超时（秒）
        queue_capacity:   队列容量（0=无界）
        submitted_count:  累计提交任务数
        completed_count:  累计完成任务数
        failed_count:     累计失败任务数
        rejected_count:   累计拒绝任务数
        avg_elapsed:      任务平均执行耗时（秒）
        utilization:      线程利用率（活跃线程/最大线程数）
    """

    active_threads: int
    core_workers: int
    max_workers: int
    pending_tasks: int
    keep_alive: float
    queue_capacity: int
    submitted_count: int
    completed_count: int
    failed_count: int
    rejected_count: int
    avg_elapsed: float
    utilization: float


# ─── 拒绝策略 ───

class RejectedExecutionError(RuntimeError):
    """任务被拒绝执行（线程池队列已满）"""
    pass


def reject_abort(executor: "AdaptiveExecutor", func: Callable, args: tuple, kwargs: dict) -> Any:
    """中止策略：抛出 RejectedExecutionError

    调用方立刻知道系统过载，可自行决定降级或重试。
    """
    raise RejectedExecutionError(
        f"Thread pool queue is full (capacity={executor._queue_capacity}, "
        f"pending={executor._work_queue.qsize()}, "
        f"active_threads={len(executor._threads)})"
    )


def reject_caller_runs(executor: "AdaptiveExecutor", func: Callable, args: tuple, kwargs: dict) -> Any:
    """调用方执行策略：由提交任务的线程自己执行

    自动降速：调用方线程被占用执行任务，无法继续提交新任务，
    线程池有时间消化队列。等同于背压（backpressure）。
    """
    return func(*args, **kwargs)


def reject_discard(executor: "AdaptiveExecutor", func: Callable, args: tuple, kwargs: dict) -> Any:
    """丢弃策略：静默丢弃新任务，不抛异常"""
    return None


def reject_discard_oldest(executor: "AdaptiveExecutor", func: Callable, args: tuple, kwargs: dict) -> Any:
    """丢弃最旧策略：从队列头部取出一个旧任务丢弃，再将新任务入队

    让新任务优先级高于旧任务，适用于实时性要求高的场景。
    旧任务被丢弃后，其 Future 会被取消。
    """
    try:
        old_item = executor._work_queue.get_nowait()
        if old_item is not None and hasattr(old_item, 'future'):
            old_item.future.cancel()
    except queue.Empty:
        pass
    # 队列已腾出一个空位，直接入队（不会再次 Full）
    from concurrent.futures.thread import _WorkItem
    f = _base.Future()
    w = _WorkItem(f, func, args, kwargs)
    executor._work_queue.put_nowait(w)
    return f


# ─── Worker 函数 ───

def _worker_core(
    executor_reference: Any,
    work_queue: queue.Queue,
    initializer: Any,
    initargs: tuple,
) -> None:
    """核心线程 worker — 无空闲超时，永远存活等待新任务"""
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException:
            _base.LOGGER.critical("Exception in initializer:", exc_info=True)
            executor = executor_reference()
            if executor is not None:
                executor._initializer_failed()
            return

    try:
        while True:
            try:
                work_item = work_queue.get_nowait()
            except queue.Empty:
                executor = executor_reference()
                if executor is not None:
                    executor._idle_semaphore.release()
                del executor
                work_item = work_queue.get(block=True)

            if work_item is not None:
                _run_work_item(executor_reference, work_item)
                del work_item
                continue

            # shutdown 信号
            executor = executor_reference()
            if _shutdown or executor is None or executor._shutdown:
                if executor is not None:
                    executor._shutdown = True
                work_queue.put(None)
            if executor is not None:
                with executor._shutdown_lock:
                    executor._threads.discard(threading.current_thread())
            del executor
            return
    except BaseException:
        _base.LOGGER.critical("Exception in core worker:", exc_info=True)
        executor = executor_reference()
        if executor is not None:
            with executor._shutdown_lock:
                executor._threads.discard(threading.current_thread())


def _worker_temporary(
    executor_reference: Any,
    work_queue: queue.Queue,
    keep_alive: float,
    initializer: Any,
    initargs: tuple,
) -> None:
    """临时线程 worker — 空闲超过 keep_alive 秒后自动退出"""
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException:
            _base.LOGGER.critical("Exception in initializer:", exc_info=True)
            executor = executor_reference()
            if executor is not None:
                executor._initializer_failed()
            if executor is not None:
                executor._threads.discard(threading.current_thread())
            return

    current_thread = threading.current_thread()

    try:
        while True:
            try:
                work_item = work_queue.get_nowait()
            except queue.Empty:
                executor = executor_reference()
                if executor is not None:
                    executor._idle_semaphore.release()
                del executor
                try:
                    work_item = work_queue.get(block=True, timeout=keep_alive)
                except queue.Empty:
                    executor = executor_reference()
                    if executor is not None:
                        with executor._shutdown_lock:
                            executor._threads.discard(current_thread)
                    del executor
                    return

            if work_item is not None:
                _run_work_item(executor_reference, work_item)
                del work_item
                continue

            # shutdown 信号
            executor = executor_reference()
            if _shutdown or executor is None or executor._shutdown:
                if executor is not None:
                    executor._shutdown = True
                work_queue.put(None)
            if executor is not None:
                with executor._shutdown_lock:
                    executor._threads.discard(current_thread)
            del executor
            return
    except BaseException:
        _base.LOGGER.critical("Exception in temporary worker:", exc_info=True)
        executor = executor_reference()
        if executor is not None:
            with executor._shutdown_lock:
                executor._threads.discard(current_thread)


def _run_work_item(executor_reference: Any, work_item: Any) -> None:
    """执行工作项，并更新监控指标和触发回调"""
    executor = executor_reference()
    start_time = time.monotonic()

    try:
        work_item.run()
    finally:
        elapsed = time.monotonic() - start_time
        if executor is not None:
            with executor._stats_lock:
                executor._completed_count += 1
                executor._total_elapsed += elapsed
            # 触发完成回调
            executor._fire_callback('on_complete', work_item, elapsed)


class AdaptiveExecutor(ThreadPoolExecutor):
    """自适应线程池 — Java ThreadPoolExecutor 模型

    参数说明：
        core_workers:      核心线程数，始终存活不回收
        max_workers:       最大线程数（核心+临时），高负载时创建临时线程
        keep_alive:        临时线程空闲超时（秒），超时后自动退出
        queue_capacity:    任务队列容量，0 表示无界队列（兼容旧行为）
        rejection_policy:  队列满时的拒绝策略
        thread_name_prefix: 线程名前缀

    拒绝策略：
        reject_abort:          抛出 RejectedExecutionError（默认）
        reject_caller_runs:    由提交线程自己执行（背压）
        reject_discard:        静默丢弃
        reject_discard_oldest: 丢弃队列中最旧的任务

    回调钩子：
        on_submit:    任务提交时触发
        on_complete:  任务完成时触发
        on_reject:    任务被拒绝时触发

    扩缩策略：
        1. 提交任务时，优先唤醒空闲核心线程
        2. 核心线程全忙 + 有排队任务 → 创建临时线程（不超过 max_workers）
        3. 临时线程空闲超过 keep_alive → 自动退出，线程数回归到 core_workers
    """

    def __init__(
        self,
        core_workers: int = 4,
        max_workers: int = 32,
        keep_alive: float = 60.0,
        queue_capacity: int = 0,
        rejection_policy: Callable[["AdaptiveExecutor", Callable, tuple, dict], Any] = reject_abort,
        thread_name_prefix: str = "invoker-worker",
        initializer: Any = None,
        initargs: tuple = (),
    ):
        if max_workers < core_workers:
            raise ValueError(
                f"max_workers ({max_workers}) must be >= core_workers ({core_workers})"
            )
        super().__init__(
            max_workers=max_workers,
            thread_name_prefix=thread_name_prefix,
            initializer=initializer,
            initargs=initargs,
        )
        self._core_workers = core_workers
        self._keep_alive = keep_alive
        self._queue_capacity = queue_capacity
        self._rejection_policy = rejection_policy

        # 监控指标
        self._stats_lock = threading.Lock()
        self._submitted_count = 0
        self._completed_count = 0
        self._failed_count = 0
        self._rejected_count = 0
        self._total_elapsed = 0.0

        # 回调钩子
        self._callbacks: Dict[str, List[Callable]] = {
            'on_submit': [],
            'on_complete': [],
            'on_reject': [],
        }

        # 替换父类的无界 SimpleQueue 为有界 Queue
        if queue_capacity > 0:
            self._work_queue = queue.Queue(maxsize=queue_capacity)

    # ─── 回调钩子 ───

    def add_callback(self, event: str, callback: Callable) -> None:
        """注册回调钩子

        Args:
            event: 事件名称，可选 on_submit / on_complete / on_reject
            callback: 回调函数，签名取决于事件类型：
                on_submit(executor, func, args, kwargs)
                on_complete(executor, work_item, elapsed)
                on_reject(executor, func, args, kwargs)
        """
        if event not in self._callbacks:
            raise ValueError(f"Unknown event: {event}, must be one of {list(self._callbacks.keys())}")
        self._callbacks[event].append(callback)

    def remove_callback(self, event: str, callback: Callable) -> None:
        """移除回调钩子"""
        if event in self._callbacks and callback in self._callbacks[event]:
            self._callbacks[event].remove(callback)

    def _fire_callback(self, event: str, *args, **kwargs) -> None:
        """触发回调（不阻塞主流程，异常只记录不抛出）"""
        for cb in self._callbacks.get(event, []):
            try:
                cb(self, *args, **kwargs)
            except Exception:
                _base.LOGGER.exception(f"Exception in {event} callback:")

    # ─── 任务提交 ───

    def submit(self, fn, /, *args, **kwargs):
        """提交任务，队列满时触发拒绝策略"""
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")

            f = _base.Future()
            from concurrent.futures.thread import _WorkItem
            w = _WorkItem(f, fn, args, kwargs)

            # 注册 Future 完成回调，追踪失败数
            f.add_done_callback(self._on_future_done)

            # 更新提交计数
            with self._stats_lock:
                self._submitted_count += 1

            # 有界队列：尝试入队，满则触发拒绝策略
            if self._queue_capacity > 0:
                try:
                    self._work_queue.put_nowait(w)
                except queue.Full:
                    with self._stats_lock:
                        self._rejected_count += 1
                    # 触发拒绝回调
                    self._fire_callback('on_reject', fn, args, kwargs)
                    result = self._rejection_policy(self, fn, args, kwargs)
                    if result is not None and not isinstance(result, _base.Future):
                        f.set_result(result)
                        return f
                    if result is None:
                        f.cancel()
                        return f
                    return result
            else:
                self._work_queue.put(w)

            # 触发提交回调
            self._fire_callback('on_submit', fn, args, kwargs)

            self._adjust_thread_count()
            return f

    # ─── 内部回调 ───

    def _on_future_done(self, future: _base.Future) -> None:
        """Future 完成回调，追踪失败数"""
        if future.exception() is not None:
            with self._stats_lock:
                self._failed_count += 1

    # ─── 线程管理 ───

    def _adjust_thread_count(self) -> None:
        """替换父类的线程创建策略，区分核心线程和临时线程"""
        if self._idle_semaphore.acquire(timeout=0):
            return

        num_threads = len(self._threads)
        pending = self._work_queue.qsize()

        if num_threads < self._core_workers:
            self._spawn_worker(timeout=None)
        elif pending > 0 and num_threads < self._max_workers:
            self._spawn_worker(timeout=self._keep_alive)

    def _spawn_worker(self, timeout: float = None) -> None:
        """创建工作线程"""
        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        thread_name = "%s_%d" % (self._thread_name_prefix or self, num_threads)

        if timeout is None:
            target = _worker_core
            args = (
                weakref.ref(self, weakref_cb),
                self._work_queue,
                self._initializer,
                self._initargs,
            )
        else:
            target = _worker_temporary
            args = (
                weakref.ref(self, weakref_cb),
                self._work_queue,
                timeout,
                self._initializer,
                self._initargs,
            )

        t = threading.Thread(name=thread_name, target=target, args=args, daemon=True)
        t.start()
        self._threads.add(t)
        _threads_queues[t] = self._work_queue

    def shutdown(self, wait=True, *, cancel_futures=False):
        """关闭线程池，等待所有工作线程退出"""
        with self._shutdown_lock:
            self._shutdown = True
        threads_count = len(self._threads)
        for _ in range(threads_count):
            try:
                self._work_queue.put(None)
            except Exception:
                pass

        if wait:
            while True:
                with self._shutdown_lock:
                    threads = list(self._threads)
                if not threads:
                    break
                for t in threads:
                    t.join(timeout=0.5)
                with self._shutdown_lock:
                    if not self._threads:
                        break

    # ─── 监控指标 ───

    @property
    def stats(self) -> ExecutorStats:
        """当前线程池状态快照（用于监控/调试）"""
        with self._stats_lock:
            submitted = self._submitted_count
            completed = self._completed_count
            failed = self._failed_count
            rejected = self._rejected_count
            total_elapsed = self._total_elapsed

        active = len(self._threads)
        avg_elapsed = total_elapsed / completed if completed > 0 else 0.0

        return ExecutorStats(
            active_threads=active,
            core_workers=self._core_workers,
            max_workers=self._max_workers,
            pending_tasks=self._work_queue.qsize(),
            keep_alive=self._keep_alive,
            queue_capacity=self._queue_capacity,
            submitted_count=submitted,
            completed_count=completed,
            failed_count=failed,
            rejected_count=rejected,
            avg_elapsed=round(avg_elapsed, 6),
            utilization=round(active / self._max_workers, 4) if self._max_workers > 0 else 0.0,
        )

    def reset_stats(self) -> None:
        """重置累计监控指标（不影响 active_threads、pending_tasks 等实时指标）"""
        with self._stats_lock:
            self._submitted_count = 0
            self._completed_count = 0
            self._failed_count = 0
            self._rejected_count = 0
            self._total_elapsed = 0.0