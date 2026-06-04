"""自适应线程池 — Java ThreadPoolExecutor 模型

核心线程始终存活，临时线程空闲超时后自动回收。
有界队列 + 拒绝策略，防止突发负载导致 OOM。
继承 ThreadPoolExecutor，复用 Future 管理逻辑，
只替换线程创建策略、队列类型和拒绝策略。
"""

import queue
import threading
import weakref

from concurrent.futures import ThreadPoolExecutor, _base

from typing_extensions import Any, Callable, Dict


# ─── 模块级全局状态 ───
# 与 concurrent.futures.thread 保持一致，用于 interpreter shutdown 通知
_threads_queues: Dict[threading.Thread, queue.SimpleQueue] = weakref.WeakKeyDictionary()
_shutdown: bool = False
_global_shutdown_lock: threading.Lock = threading.Lock()


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
    """核心线程 worker — 无空闲超时，永远存活等待新任务

    与标准库 _worker 的区别：
      标准 _worker 用 work_queue.get(block=True) 无限等待，
      但没有区分核心/临时线程的概念。

      本 worker 是核心线程的专属逻辑：
      - 从队列取任务时无限等待（不超时退出）
      - 只有 executor shutdown 或 interpreter shutdown 时才退出
    """
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
            # 先尝试无阻塞取任务（快速路径）
            try:
                work_item = work_queue.get_nowait()
            except queue.Empty:
                # 队列空 → 通知 idle_semaphore（有空闲线程了）
                executor = executor_reference()
                if executor is not None:
                    executor._idle_semaphore.release()
                del executor
                # 核心线程：无限等待新任务
                work_item = work_queue.get(block=True)

            if work_item is not None:
                work_item.run()
                del work_item
                continue

            # work_item is None → shutdown 信号
            executor = executor_reference()
            if _shutdown or executor is None or executor._shutdown:
                if executor is not None:
                    executor._shutdown = True
                # 通知其他 worker 退出
                work_queue.put(None)
            # shutdown 时也要从 threads 中移除
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
    """临时线程 worker — 空闲超过 keep_alive 秒后自动退出

    与核心线程的区别：
      - 队列空时用 work_queue.get(block=True, timeout=keep_alive) 等待
      - 超时后从 executor._threads 中移除自己并退出
    """
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException:
            _base.LOGGER.critical("Exception in initializer:", exc_info=True)
            executor = executor_reference()
            if executor is not None:
                executor._initializer_failed()
            # 临时线程初始化失败也要从 threads 中移除
            if executor is not None:
                executor._threads.discard(threading.current_thread())
            return

    current_thread = threading.current_thread()

    try:
        while True:
            # 先尝试无阻塞取任务（快速路径）
            try:
                work_item = work_queue.get_nowait()
            except queue.Empty:
                # 队列空 → 通知 idle_semaphore
                executor = executor_reference()
                if executor is not None:
                    executor._idle_semaphore.release()
                del executor
                # 临时线程：等待新任务，超时后退出
                try:
                    work_item = work_queue.get(block=True, timeout=keep_alive)
                except queue.Empty:
                    # 空闲超时 → 从 threads 中移除自己并退出
                    executor = executor_reference()
                    if executor is not None:
                        with executor._shutdown_lock:
                            executor._threads.discard(current_thread)
                    # 通知 _adjust_thread_count 有线程退出了
                    # 通过释放一个 semaphore 信号让下次 submit 时能感知
                    del executor
                    return

            if work_item is not None:
                work_item.run()
                del work_item
                continue

            # work_item is None → shutdown 信号
            executor = executor_reference()
            if _shutdown or executor is None or executor._shutdown:
                if executor is not None:
                    executor._shutdown = True
                # 通知其他 worker 退出
                work_queue.put(None)
            # shutdown 时也要从 threads 中移除
            if executor is not None:
                with executor._shutdown_lock:
                    executor._threads.discard(current_thread)
            del executor
            return
    except BaseException:
        _base.LOGGER.critical("Exception in temporary worker:", exc_info=True)
        # 异常退出也要从 threads 中移除
        executor = executor_reference()
        if executor is not None:
            with executor._shutdown_lock:
                executor._threads.discard(current_thread)


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
        # 父类的 _max_workers 设为 max_workers（上限）
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
        self._rejected_count = 0

        # 替换父类的无界 SimpleQueue 为有界 Queue
        if queue_capacity > 0:
            self._work_queue = queue.Queue(maxsize=queue_capacity)

    def submit(self, fn, /, *args, **kwargs):
        """提交任务，队列满时触发拒绝策略"""
        with self._shutdown_lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")

            # 创建 Future 和 WorkItem
            f = _base.Future()
            w = _base.WorkItem(f, fn, args, kwargs)

            # 有界队列：尝试入队，满则触发拒绝策略
            if self._queue_capacity > 0:
                try:
                    self._work_queue.put_nowait(w)
                except queue.Full:
                    self._rejected_count += 1
                    result = self._rejection_policy(self, fn, args, kwargs)
                    # reject_caller_runs 返回结果，需要包装成 Future
                    if result is not None and not isinstance(result, _base.Future):
                        f.set_result(result)
                        return f
                    # reject_discard 返回 None，返回一个已取消的 Future
                    if result is None:
                        f.cancel()
                        return f
                    return result
            else:
                # 无界队列：直接入队（兼容旧行为）
                self._work_queue.put(w)

            self._adjust_thread_count()
            return f

    def _adjust_thread_count(self) -> None:
        """替换父类的线程创建策略，区分核心线程和临时线程

        规则：
            1. 有空闲线程 → 唤醒它，不新建
            2. 线程数 < core_workers → 创建核心线程（永不超时）
            3. 线程数 >= core_workers + 有排队任务 + 未达上限 → 创建临时线程
        """
        # 有空闲线程 → 唤醒它，不新建
        if self._idle_semaphore.acquire(timeout=0):
            return

        num_threads = len(self._threads)
        pending = self._work_queue.qsize()

        if num_threads < self._core_workers:
            # 核心线程未满 → 创建核心线程
            self._spawn_worker(timeout=None)
        elif pending > 0 and num_threads < self._max_workers:
            # 核心线程满了 + 有排队任务 + 未达上限 → 创建临时线程
            self._spawn_worker(timeout=self._keep_alive)

    def _spawn_worker(self, timeout: float = None) -> None:
        """创建工作线程

        Args:
            timeout: 空闲超时时间。None 表示核心线程（永不超时退出），
                     有值表示临时线程（超时后退出）。
        """
        # executor 被回收时的回调 — 向队列发送 None 信号通知 worker 退出
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
        """关闭线程池，等待所有工作线程退出

        重写父类方法，修复 _threads 在迭代期间被修改的问题
        （临时线程退出时会从 _threads 中移除自己）
        """
        with self._shutdown_lock:
            self._shutdown = True
        # 向队列发送退出信号，数量等于当前线程数
        threads_count = len(self._threads)
        for _ in range(threads_count):
            try:
                self._work_queue.put(None)
            except Exception:
                pass

        if wait:
            # 等待所有线程退出，使用快照避免迭代期间修改
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

    @property
    def stats(self) -> Dict[str, Any]:
        """当前线程池状态（用于监控/调试）"""
        return {
            "active_threads": len(self._threads),
            "core_workers": self._core_workers,
            "max_workers": self._max_workers,
            "pending_tasks": self._work_queue.qsize(),
            "keep_alive": self._keep_alive,
            "queue_capacity": self._queue_capacity,
            "rejected_count": self._rejected_count,
        }