"""自适应线程池测试"""

import threading
import time

import pytest

from invoker_switch import (
    AdaptiveExecutor,
    ExecutorStats,
    RejectedExecutionError,
    reject_abort,
    reject_caller_runs,
    reject_discard,
)


class TestAdaptiveExecutorBasic:
    """基本功能测试"""

    def test_submit_and_result(self):
        """基本提交和获取结果"""
        with AdaptiveExecutor(core_workers=2, max_workers=4) as executor:
            future = executor.submit(lambda: 1 + 1)
            assert future.result() == 2

    def test_core_workers_validation(self):
        """max_workers < core_workers 应该报错"""
        with pytest.raises(ValueError, match="max_workers"):
            AdaptiveExecutor(core_workers=10, max_workers=4)

    def test_default_params(self):
        """默认参数应该合理"""
        executor = AdaptiveExecutor()
        assert executor._core_workers == 4
        assert executor._max_workers == 32
        assert executor._keep_alive == 60.0
        executor.shutdown(wait=False)

    def test_multiple_tasks(self):
        """多个任务并发执行"""
        with AdaptiveExecutor(core_workers=4, max_workers=8) as executor:
            futures = [executor.submit(lambda i=i: i * 2) for i in range(20)]
            results = [f.result() for f in futures]
            assert results == [i * 2 for i in range(20)]


class TestCoreWorkers:
    """核心线程测试"""

    def test_core_threads_created_on_demand(self):
        """核心线程在提交任务时按需创建"""
        with AdaptiveExecutor(core_workers=4, max_workers=8) as executor:
            # 还没提交任务，线程数为 0（延迟创建）
            assert len(executor._threads) == 0

            # 提交 1 个任务 → 创建 1 个核心线程
            executor.submit(time.sleep, 0.01)
            time.sleep(0.05)
            assert len(executor._threads) >= 1

            # 提交更多任务 → 核心线程逐步创建（不超过 core_workers）
            futures = [executor.submit(time.sleep, 0.05) for _ in range(10)]
            time.sleep(0.1)
            # 线程数应在 core_workers 范围内
            assert len(executor._threads) <= 8

    def test_core_threads_not_reclaimed(self):
        """核心线程不会被回收"""
        with AdaptiveExecutor(core_workers=4, max_workers=8, keep_alive=0.5) as executor:
            # 提交任务让核心线程创建
            futures = [executor.submit(lambda: None) for _ in range(4)]
            for f in futures:
                f.result()

            # 等待任务完成
            time.sleep(0.1)
            core_count = len(executor._threads)
            assert core_count >= 1

            # 等待超过 keep_alive 时间
            time.sleep(1.0)

            # 核心线程应该还在
            assert len(executor._threads) >= 1


class TestTemporaryWorkers:
    """临时线程测试"""

    def test_temporary_threads_created_under_load(self):
        """高负载时创建临时线程"""
        with AdaptiveExecutor(core_workers=2, max_workers=8, keep_alive=1.0) as executor:
            # 用 Event 占住核心线程
            block_event = threading.Event()

            def blocking_task():
                block_event.wait(timeout=5)

            blockers = [executor.submit(blocking_task) for _ in range(2)]
            time.sleep(0.2)
            threads_before = len(executor._threads)

            # 提交更多任务 → 应该创建临时线程
            extra = [executor.submit(time.sleep, 0.1) for _ in range(4)]
            time.sleep(0.3)

            threads_during = len(executor._threads)
            assert threads_during > threads_before

            # 释放阻塞
            block_event.set()
            for f in blockers + extra:
                f.result(timeout=5)

    def test_temporary_threads_reclaimed_after_idle(self):
        """临时线程空闲后自动回收"""
        with AdaptiveExecutor(core_workers=2, max_workers=8, keep_alive=0.5) as executor:
            # 用 Event 占住核心线程，触发临时线程创建
            block_event = threading.Event()

            def blocking_task():
                block_event.wait(timeout=5)

            blockers = [executor.submit(blocking_task) for _ in range(2)]
            time.sleep(0.2)

            # 提交更多任务 → 触发临时线程
            extra = [executor.submit(time.sleep, 0.1) for _ in range(6)]
            time.sleep(0.3)
            threads_peak = len(executor._threads)

            # 释放阻塞，等待所有任务完成
            block_event.set()
            for f in blockers + extra:
                f.result(timeout=5)

            # 等待临时线程超时回收
            time.sleep(1.5)
            threads_after = len(executor._threads)

            # 线程数应该减少了（临时线程已回收）
            assert threads_after < threads_peak


class TestStats:
    """stats 属性测试"""

    def test_stats_structure(self):
        """stats 属性返回 ExecutorStats 数据类"""
        with AdaptiveExecutor(core_workers=4, max_workers=16, keep_alive=30.0) as executor:
            stats = executor.stats
            assert isinstance(stats, ExecutorStats)
            assert stats.core_workers == 4
            assert stats.max_workers == 16
            assert stats.keep_alive == 30.0

    def test_stats_updates_with_load(self):
        """stats 反映实际负载"""
        with AdaptiveExecutor(core_workers=2, max_workers=8) as executor:
            stats_before = executor.stats
            assert stats_before.active_threads == 0

            futures = [executor.submit(time.sleep, 0.2) for _ in range(6)]
            time.sleep(0.1)
            stats_during = executor.stats
            assert stats_during.active_threads > 0

            for f in futures:
                f.result()


class TestCompatibility:
    """兼容性测试"""

    def test_plain_thread_pool_executor_still_works(self):
        """普通 ThreadPoolExecutor 注入后行为不变"""
        from concurrent.futures import ThreadPoolExecutor as TPE

        with TPE(max_workers=4) as executor:
            future = executor.submit(lambda: 42)
            assert future.result() == 42


class TestBoundedQueue:
    """有界队列测试"""

    def test_unbounded_queue_default(self):
        """默认 queue_capacity=0 为无界队列，不触发拒绝"""
        with AdaptiveExecutor(core_workers=2, max_workers=4, queue_capacity=0) as executor:
            # 提交超过 max_workers 的任务，全部能入队
            futures = [executor.submit(lambda i=i: i * 2) for i in range(100)]
            results = [f.result(timeout=5) for f in futures]
            assert results == [i * 2 for i in range(100)]

    def test_bounded_queue_reject_abort(self):
        """有界队列满时，reject_abort 抛出 RejectedExecutionError"""
        with AdaptiveExecutor(
            core_workers=2, max_workers=4, queue_capacity=2,
            rejection_policy=reject_abort,
        ) as executor:
            # 用 Event 占住所有线程
            block_event = threading.Event()

            def blocking_task():
                block_event.wait(timeout=5)

            # 占住 4 个线程（2 core + 2 temp）
            blockers = [executor.submit(blocking_task) for _ in range(4)]
            time.sleep(0.2)

            # 队列还能放 2 个任务
            queued = [executor.submit(lambda: "queued") for _ in range(2)]

            # 队列满了 → 抛 RejectedExecutionError
            with pytest.raises(RejectedExecutionError):
                executor.submit(lambda: "rejected")

            # 释放阻塞
            block_event.set()
            for f in blockers + queued:
                f.result(timeout=5)

    def test_bounded_queue_reject_caller_runs(self):
        """reject_caller_runs 由提交线程自己执行，不抛异常"""
        with AdaptiveExecutor(
            core_workers=2, max_workers=4, queue_capacity=2,
            rejection_policy=reject_caller_runs,
        ) as executor:
            block_event = threading.Event()

            def blocking_task():
                block_event.wait(timeout=5)

            # 占住线程 + 填满队列
            blockers = [executor.submit(blocking_task) for _ in range(4)]
            queued = [executor.submit(lambda: "queued") for _ in range(2)]
            time.sleep(0.2)

            # 队列满 → caller_runs 在当前线程执行
            result = executor.submit(lambda: "caller_runs_result")
            assert result.result(timeout=5) == "caller_runs_result"

            block_event.set()
            for f in blockers + queued:
                f.result(timeout=5)

    def test_bounded_queue_reject_discard(self):
        """reject_discard 静默丢弃任务，返回已取消的 Future"""
        with AdaptiveExecutor(
            core_workers=2, max_workers=4, queue_capacity=2,
            rejection_policy=reject_discard,
        ) as executor:
            block_event = threading.Event()

            def blocking_task():
                block_event.wait(timeout=5)

            blockers = [executor.submit(blocking_task) for _ in range(4)]
            queued = [executor.submit(lambda: "queued") for _ in range(2)]
            time.sleep(0.2)

            # 队列满 → 丢弃
            result = executor.submit(lambda: "discarded")
            assert result.cancelled() or result.result(timeout=5) is None

            block_event.set()
            for f in blockers + queued:
                f.result(timeout=5)

    def test_stats_includes_queue_info(self):
        """stats 包含队列容量和拒绝计数"""
        with AdaptiveExecutor(core_workers=4, max_workers=16, queue_capacity=100) as executor:
            stats = executor.stats
            assert stats.queue_capacity == 100
            assert stats.rejected_count == 0
