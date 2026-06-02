"""性能测试 — 衡量各执行路径的开销

测试维度：
1. SyncInvoker.invoke() vs 原生调用的开销对比
2. 六种执行场景的单次调用延迟
3. run_callable vs SyncInvoker.invoke 的开销对比
4. 并发场景下的吞吐量
5. 重入调用链的延迟累积
"""

import asyncio
import time
import statistics

import pytest

from invoker_switch import InvokerBase, SyncInvoker, arun_callable, run_callable


# ─── 辅助工具 ───

def _measure(func, iterations: int = 1000) -> dict:
    """测量函数执行时间，返回统计信息"""
    latencies = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        latencies.append((end - start) * 1_000_000)  # 微秒

    return {
        "mean_us": statistics.mean(latencies),
        "median_us": statistics.median(latencies),
        "p95_us": sorted(latencies)[int(len(latencies) * 0.95)],
        "min_us": min(latencies),
        "max_us": max(latencies),
    }


async def _measure_async(func, iterations: int = 1000) -> dict:
    """测量异步函数执行时间，返回统计信息

    自动处理返回值：如果是协程则 await，否则直接取值。
    """
    latencies = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func()
        if asyncio.iscoroutine(result):
            await result
        end = time.perf_counter()
        latencies.append((end - start) * 1_000_000)

    return {
        "mean_us": statistics.mean(latencies),
        "median_us": statistics.median(latencies),
        "p95_us": sorted(latencies)[int(len(latencies) * 0.95)],
        "min_us": min(latencies),
        "max_us": max(latencies),
    }


# ─── 测试用 Service ───


class PerfService(InvokerBase):
    """性能测试服务"""

    def noop_sync(self) -> None:
        """空操作同步方法"""
        pass

    async def noop_async(self) -> None:
        """空操作异步方法"""
        pass

    def compute_sync(self, x: int) -> int:
        """轻量计算同步方法"""
        return x * 2

    async def compute_async(self, x: int) -> int:
        """轻量计算异步方法"""
        return x * 2


# ─── 1. 原生调用 vs SyncInvoker 开销 ───


class TestInvokerOverhead:
    """衡量 SyncInvoker.invoke() 相比原生调用的额外开销"""

    ITERATIONS = 500

    def test_sync_overhead(self):
        """同步方法：原生调用 vs SyncInvoker 开销"""
        svc = PerfService()

        # 原生直接调用（绕过 SyncInvoker）
        def native_call():
            PerfService.noop_sync.__wrapped__(svc)

        # 通过 SyncInvoker 调用
        def invoker_call():
            svc.noop_sync()

        native_stats = _measure(native_call, self.ITERATIONS)
        invoker_stats = _measure(invoker_call, self.ITERATIONS)

        overhead_us = invoker_stats["mean_us"] - native_stats["mean_us"]
        print(f"\n  [同步方法开销]")
        print(f"  原生调用:   {native_stats['mean_us']:.1f} μs (中位数 {native_stats['median_us']:.1f})")
        print(f"  SyncInvoker: {invoker_stats['mean_us']:.1f} μs (中位数 {invoker_stats['median_us']:.1f})")
        print(f"  额外开销:   {overhead_us:.1f} μs")

        # SyncInvoker 不应比原生慢超过 2ms（含线程池调度）
        assert overhead_us < 2000, f"SyncInvoker overhead too high: {overhead_us:.1f} μs"

    def test_async_overhead_in_sync_context(self):
        """同步上下文中调用异步方法：原生 asyncio.run vs SyncInvoker"""
        svc = PerfService()

        def invoker_call():
            svc.noop_async()

        invoker_stats = _measure(invoker_call, self.ITERATIONS)

        print(f"\n  [同步上下文 + 异步方法]")
        print(f"  SyncInvoker: {invoker_stats['mean_us']:.1f} μs (中位数 {invoker_stats['median_us']:.1f})")

        # 不应超过 5ms（含事件循环调度）
        assert invoker_stats["mean_us"] < 5000


# ─── 2. 六种执行场景延迟 ───


class TestScenarioLatency:
    """六种执行场景的单次调用延迟"""

    ITERATIONS = 500

    def test_sync_context_sync_method(self):
        """场景 1：同步上下文 + 同步方法"""
        svc = PerfService()
        stats = _measure(lambda: svc.noop_sync(), self.ITERATIONS)
        print(f"\n  [场景1] 同步+同步: {stats['mean_us']:.1f} μs (P95: {stats['p95_us']:.1f})")
        assert stats["mean_us"] < 2000

    def test_sync_context_async_method(self):
        """场景 2：同步上下文 + 异步方法"""
        svc = PerfService()
        stats = _measure(lambda: svc.noop_async(), self.ITERATIONS)
        print(f"\n  [场景2] 同步+异步: {stats['mean_us']:.1f} μs (P95: {stats['p95_us']:.1f})")
        assert stats["mean_us"] < 5000

    async def test_async_context_async_method(self):
        """场景 3：异步上下文 + 异步方法"""
        svc = PerfService()
        stats = await _measure_async(lambda: svc.noop_async(), self.ITERATIONS)
        print(f"\n  [场景3] 异步+异步: {stats['mean_us']:.1f} μs (P95: {stats['p95_us']:.1f})")
        assert stats["mean_us"] < 1000

    async def test_async_context_sync_method_with_await(self):
        """场景 5：异步上下文 + 同步方法 + await"""
        svc = PerfService()
        # 必须在 lambda 中使用 await，才能触发 _execute_sync_as_coro
        async def _call():
            await svc.noop_sync()
        stats = await _measure_async(_call, self.ITERATIONS)
        print(f"\n  [场景5] 异步+同步+await: {stats['mean_us']:.1f} μs (P95: {stats['p95_us']:.1f})")
        assert stats["mean_us"] < 3000

    async def test_async_context_sync_method_no_await(self):
        """场景 6：异步上下文 + 同步方法 + 无 await"""
        svc = PerfService()
        stats = await _measure_async(lambda: svc.noop_sync(), self.ITERATIONS)
        print(f"\n  [场景6] 异步+同步+无await: {stats['mean_us']:.1f} μs (P95: {stats['p95_us']:.1f})")
        assert stats["mean_us"] < 3000


# ─── 3. run_callable vs SyncInvoker.invoke 开销对比 ───


class TestRunCallableOverhead:
    """run_callable 函数式调用 vs SyncInvoker 声明式调用的开销对比"""

    ITERATIONS = 500

    def test_sync_context_sync_func(self):
        """同步上下文 + 同步函数：run_callable vs SyncInvoker"""
        svc = PerfService()

        def invoker_call():
            svc.compute_sync(42)

        def run_callable_call():
            run_callable(lambda: 42 * 2)

        invoker_stats = _measure(invoker_call, self.ITERATIONS)
        rc_stats = _measure(run_callable_call, self.ITERATIONS)

        print(f"\n  [同步+同步] run_callable: {rc_stats['mean_us']:.1f} μs, SyncInvoker: {invoker_stats['mean_us']:.1f} μs")

    def test_sync_context_async_func(self):
        """同步上下文 + 异步函数：run_callable vs SyncInvoker"""
        svc = PerfService()

        async def _compute():
            return 42 * 2

        def invoker_call():
            svc.compute_async(42)

        def run_callable_call():
            run_callable(_compute)

        invoker_stats = _measure(invoker_call, self.ITERATIONS)
        rc_stats = _measure(run_callable_call, self.ITERATIONS)

        print(f"\n  [同步+异步] run_callable: {rc_stats['mean_us']:.1f} μs, SyncInvoker: {invoker_stats['mean_us']:.1f} μs")

    async def test_async_context_sync_func(self):
        """异步上下文 + 同步函数：arun_callable vs SyncInvoker"""
        svc = PerfService()

        async def _invoker_call():
            await svc.compute_sync(42)

        async def _rc_call():
            await arun_callable(lambda: 42 * 2)

        invoker_stats = await _measure_async(_invoker_call, self.ITERATIONS)
        rc_stats = await _measure_async(_rc_call, self.ITERATIONS)

        print(f"\n  [异步+同步] arun_callable: {rc_stats['mean_us']:.1f} μs, SyncInvoker: {invoker_stats['mean_us']:.1f} μs")

    async def test_async_context_async_func(self):
        """异步上下文 + 异步函数：arun_callable vs SyncInvoker"""
        svc = PerfService()

        async def _compute():
            return 42 * 2

        async def _invoker_call():
            await svc.compute_async(42)

        async def _rc_call():
            await arun_callable(_compute)

        invoker_stats = await _measure_async(_invoker_call, self.ITERATIONS)
        rc_stats = await _measure_async(_rc_call, self.ITERATIONS)

        print(f"\n  [异步+异步] arun_callable: {rc_stats['mean_us']:.1f} μs, SyncInvoker: {invoker_stats['mean_us']:.1f} μs")


# ─── 4. 并发吞吐量 ───


class TestConcurrencyThroughput:
    """并发场景下的吞吐量"""

    async def test_async_concurrent_invocations(self):
        """异步上下文中并发调用 SyncInvoker 的吞吐量"""
        svc = PerfService()
        concurrency = 100
        iterations = 10

        start = time.perf_counter()
        for _ in range(iterations):
            await asyncio.gather(*[svc.noop_async() for _ in range(concurrency)])
        elapsed = time.perf_counter() - start

        total_calls = concurrency * iterations
        throughput = total_calls / elapsed
        print(f"\n  [异步并发吞吐量] {throughput:.0f} calls/sec ({total_calls} calls in {elapsed:.3f}s)")

        # 至少 1000 calls/sec
        assert throughput > 1000

    async def test_run_callable_concurrent(self):
        """异步上下文中并发调用 arun_callable 的吞吐量"""
        concurrency = 100
        iterations = 10

        async def _noop():
            pass

        start = time.perf_counter()
        for _ in range(iterations):
            await asyncio.gather(*[arun_callable(_noop) for _ in range(concurrency)])
        elapsed = time.perf_counter() - start

        total_calls = concurrency * iterations
        throughput = total_calls / elapsed
        print(f"\n  [arun_callable 吞吐量] {throughput:.0f} calls/sec ({total_calls} calls in {elapsed:.3f}s)")

        assert throughput > 1000


# ─── 5. 重入调用链延迟 ───


class ReentrantPerfService(InvokerBase):
    """重入性能测试服务"""

    def entry(self) -> str:
        return self.async_step()

    async def async_step(self) -> str:
        r = await self.sync_step()
        return f"async->{r}"

    def sync_step(self) -> str:
        return self.async_final()

    async def async_final(self) -> str:
        return "done"


class TestReentrantLatency:
    """重入调用链（sync→async→sync→async）的延迟累积"""

    ITERATIONS = 200

    def test_reentrant_chain_latency(self):
        """完整重入链的延迟"""
        svc = ReentrantPerfService()
        stats = _measure(lambda: svc.entry(), self.ITERATIONS)

        print(f"\n  [重入链 sync→async→sync→async]")
        print(f"  延迟: {stats['mean_us']:.1f} μs (中位数 {stats['median_us']:.1f}, P95 {stats['p95_us']:.1f})")

        # 4 层调用不应超过 20ms
        assert stats["mean_us"] < 20000
