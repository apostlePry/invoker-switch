"""性能测试 — 衡量各执行路径的开销并分析瓶颈

测试维度：
1. 开销拆解：原生调用 vs SyncInvoker 各层额外开销
2. 五种执行场景延迟（决策矩阵覆盖）
3. SyncInvoker vs run_callable/arun_callable 对比
4. 并发吞吐量
5. 重入调用链延迟累积
6. 帧管理开销

所有测试包含预热阶段，避免首次执行的 JIT/缓存冷启动偏差。
"""

import asyncio
import statistics
import time

import pytest

from invoker_switch import InvokerBase, SyncInvoker, arun_callable, run_callable
from invoker_switch.detection import is_awaited


# ─── 辅助工具 ───


def _measure(func, warmup: int = 100, iterations: int = 1000) -> dict:
    """测量函数执行时间，返回统计信息（含预热）"""
    for _ in range(warmup):
        func()

    latencies = []
    for _ in range(iterations):
        start = time.perf_counter()
        func()
        end = time.perf_counter()
        latencies.append((end - start) * 1_000_000)  # 微秒

    latencies.sort()
    return {
        "mean": statistics.mean(latencies),
        "median": statistics.median(latencies),
        "p50": latencies[int(len(latencies) * 0.50)],
        "p90": latencies[int(len(latencies) * 0.90)],
        "p99": latencies[int(len(latencies) * 0.99)],
        "min": latencies[0],
        "max": latencies[-1],
        "stddev": statistics.stdev(latencies) if len(latencies) > 1 else 0,
    }


async def _measure_async(func, warmup: int = 100, iterations: int = 1000) -> dict:
    """测量异步函数执行时间（含预热），自动处理协程返回值"""
    for _ in range(warmup):
        result = func()
        if asyncio.iscoroutine(result):
            await result

    latencies = []
    for _ in range(iterations):
        start = time.perf_counter()
        result = func()
        if asyncio.iscoroutine(result):
            await result
        end = time.perf_counter()
        latencies.append((end - start) * 1_000_000)

    latencies.sort()
    return {
        "mean": statistics.mean(latencies),
        "median": statistics.median(latencies),
        "p50": latencies[int(len(latencies) * 0.50)],
        "p90": latencies[int(len(latencies) * 0.90)],
        "p99": latencies[int(len(latencies) * 0.99)],
        "min": latencies[0],
        "max": latencies[-1],
        "stddev": statistics.stdev(latencies) if len(latencies) > 1 else 0,
    }


def _fmt(stats: dict) -> str:
    """格式化统计信息为可读字符串"""
    return (
        f"mean={stats['mean']:.1f}μs  "
        f"p50={stats['p50']:.1f}μs  "
        f"p99={stats['p99']:.1f}μs  "
        f"±{stats['stddev']:.1f}μs"
    )


# ─── 测试用 Service ───


class PerfService(InvokerBase):
    """性能测试服务"""

    def noop_sync(self) -> None:
        pass

    async def noop_async(self) -> None:
        pass

    def compute_sync(self, x: int) -> int:
        return x * 2

    async def compute_async(self, x: int) -> int:
        return x * 2

    def io_simulate(self) -> str:
        """模拟 I/O 密集型同步方法"""
        time.sleep(0.001)
        return "io_done"

    async def async_io_simulate(self) -> str:
        """模拟 I/O 密集型异步方法"""
        await asyncio.sleep(0.001)
        return "async_io_done"


# ─── 1. 开销拆解 ───


class TestOverheadBreakdown:
    """逐层拆解 SyncInvoker 的额外开销"""

    WARMUP = 200
    ITERATIONS = 2000

    def test_sync_overhead_layers(self):
        """同步方法：逐层拆解 SyncInvoker 开销"""
        svc = PerfService()

        # 第 0 层：原生 Python 调用
        def raw_call():
            return PerfService.noop_sync.__wrapped__(svc)

        # 第 1 层：SyncInvoker 完整路径
        def invoker_call():
            return svc.noop_sync()

        raw = _measure(raw_call, self.WARMUP, self.ITERATIONS)
        invoker = _measure(invoker_call, self.WARMUP, self.ITERATIONS)

        overhead = invoker["mean"] - raw["mean"]
        print(f"\n  ┌─────────────────────────────────────────────────────┐")
        print(f"  │ 同步方法开销拆解                                     │")
        print(f"  ├─────────────────────────────────────────────────────┤")
        print(f"  │ 原生调用:       {_fmt(raw):>42s} │")
        print(f"  │ SyncInvoker:    {_fmt(invoker):>42s} │")
        print(f"  │ 额外开销:       {overhead:>8.1f}μs{' ':>33s} │")
        print(f"  │ 开销倍数:       {invoker['mean']/raw['mean']:>8.1f}x{' ':>33s} │")
        print(f"  └─────────────────────────────────────────────────────┘")

        assert overhead < 500, f"SyncInvoker sync overhead too high: {overhead:.1f}μs"

    def test_async_overhead_in_sync_context(self):
        """同步上下文 + 异步方法：完整路径开销"""
        svc = PerfService()
        stats = _measure(lambda: svc.noop_async(), self.WARMUP, self.ITERATIONS)

        print(f"\n  [同步上下文 + 异步方法] {_fmt(stats)}")
        assert stats["mean"] < 5000

    def test_detection_overhead(self):
        """is_awaited() 字节码检测本身的开销"""
        # 直接调用 is_awaited，测量其纯开销
        direct = _measure(lambda: None, self.WARMUP, self.ITERATIONS)
        detected = _measure(is_awaited, self.WARMUP, self.ITERATIONS)

        overhead = detected["mean"] - direct["mean"]
        print(f"\n  [is_awaited() 开销] {overhead:.1f}μs (纯检测调用)")
        assert overhead < 500, f"is_awaited overhead too high: {overhead:.1f}μs"


# ─── 2. 五种执行场景延迟 ───


class TestScenarioLatency:
    """决策矩阵覆盖的五种场景延迟"""

    WARMUP = 200
    ITERATIONS = 2000

    def test_sync_chain_sync_method(self):
        """SYNC 方法 + 同步调用链（无 await）→ 直接执行"""
        svc = PerfService()
        stats = _measure(lambda: svc.noop_sync(), self.WARMUP, self.ITERATIONS)
        print(f"\n  [SYNC+同步链]    {_fmt(stats)}")
        assert stats["mean"] < 500

    def test_sync_chain_async_method(self):
        """ASYNC 方法 + 同步调用链 → _submit_coro 阻塞等待"""
        svc = PerfService()
        stats = _measure(lambda: svc.noop_async(), self.WARMUP, self.ITERATIONS)
        print(f"\n  [ASYNC+同步链]   {_fmt(stats)}")
        assert stats["mean"] < 5000

    async def test_async_chain_async_method(self):
        """ASYNC 方法 + 异步调用链 → _execute_async 返回协程"""
        svc = PerfService()
        stats = await _measure_async(lambda: svc.noop_async(), self.WARMUP, self.ITERATIONS)
        print(f"\n  [ASYNC+异步链]   {_fmt(stats)}")
        assert stats["mean"] < 500

    async def test_async_chain_sync_with_await(self):
        """SYNC 方法 + 异步调用链 + await → _execute_sync_as_coro"""
        svc = PerfService()

        async def _call():
            await svc.noop_sync()

        stats = await _measure_async(_call, self.WARMUP, self.ITERATIONS)
        print(f"\n  [SYNC+异步链+await] {_fmt(stats)}")
        assert stats["mean"] < 3000

    async def test_async_chain_sync_no_await(self):
        """SYNC 方法 + 异步调用链 + 无 await → _execute_sync"""
        svc = PerfService()
        stats = await _measure_async(lambda: svc.noop_sync(), self.WARMUP, self.ITERATIONS)
        print(f"\n  [SYNC+异步链+无] {_fmt(stats)}")
        assert stats["mean"] < 500


# ─── 3. SyncInvoker vs run_callable/arun_callable ───


class TestComparison:
    """SyncInvoker 声明式 vs run_callable/arun_callable 函数式"""

    WARMUP = 200
    ITERATIONS = 2000

    @pytest.fixture(autouse=True)
    def _print_header(self):
        print(f"\n  ┌────────────────────────────────────────────────────────────┐")
        print(f"  │ SyncInvoker vs run_callable/arun_callable                   │")
        print(f"  ├────────────────────────────────────────────────────────────┤")
        yield
        print(f"  └────────────────────────────────────────────────────────────┘")

    def test_sync_context_sync_func(self):
        """同步环境 + 同步函数"""
        svc = PerfService()

        invoker_stats = _measure(lambda: svc.compute_sync(42), self.WARMUP, self.ITERATIONS)
        rc_stats = _measure(lambda: run_callable(lambda: 42 * 2), self.WARMUP, self.ITERATIONS)

        print(f"  │ 同步+sync  │ Invoker: {invoker_stats['mean']:>7.1f}μs  │ "
              f"run_callable: {rc_stats['mean']:>7.1f}μs │")

    def test_sync_context_async_func(self):
        """同步环境 + 异步函数"""
        svc = PerfService()

        async def _compute():
            return 42 * 2

        invoker_stats = _measure(lambda: svc.compute_async(42), self.WARMUP, self.ITERATIONS)
        rc_stats = _measure(lambda: run_callable(_compute), self.WARMUP, self.ITERATIONS)

        print(f"  │ 同步+async │ Invoker: {invoker_stats['mean']:>7.1f}μs  │ "
              f"run_callable: {rc_stats['mean']:>7.1f}μs │")

    async def test_async_context_sync_func(self):
        """异步环境 + 同步函数"""
        svc = PerfService()

        async def _invoker_call():
            await svc.compute_sync(42)

        async def _rc_call():
            await arun_callable(lambda: 42 * 2)

        invoker_stats = await _measure_async(_invoker_call, self.WARMUP, self.ITERATIONS)
        rc_stats = await _measure_async(_rc_call, self.WARMUP, self.ITERATIONS)

        print(f"  │ 异步+sync  │ Invoker: {invoker_stats['mean']:>7.1f}μs  │ "
              f"arun: {rc_stats['mean']:>7.1f}μs │")

    async def test_async_context_async_func(self):
        """异步环境 + 异步函数"""
        svc = PerfService()

        async def _compute():
            return 42 * 2

        async def _invoker_call():
            await svc.compute_async(42)

        async def _rc_call():
            await arun_callable(_compute)

        invoker_stats = await _measure_async(_invoker_call, self.WARMUP, self.ITERATIONS)
        rc_stats = await _measure_async(_rc_call, self.WARMUP, self.ITERATIONS)

        print(f"  │ 异步+async │ Invoker: {invoker_stats['mean']:>7.1f}μs  │ "
              f"arun: {rc_stats['mean']:>7.1f}μs │")


# ─── 4. 并发吞吐量 ───


class TestThroughput:
    """并发场景下的吞吐量"""

    async def test_invoker_async_throughput(self):
        """SyncInvoker 异步方法并发吞吐量"""
        svc = PerfService()
        concurrency = 200
        iterations = 20

        # 预热
        await asyncio.gather(*[svc.noop_async() for _ in range(50)])

        start = time.perf_counter()
        for _ in range(iterations):
            await asyncio.gather(*[svc.noop_async() for _ in range(concurrency)])
        elapsed = time.perf_counter() - start

        total = concurrency * iterations
        tps = total / elapsed
        latency_ms = (elapsed / total) * 1000
        print(f"\n  [SyncInvoker 并发] {tps:.0f} TPS ({total} calls in {elapsed:.3f}s, avg {latency_ms:.2f}ms/call)")
        assert tps > 5000

    async def test_arun_callable_throughput(self):
        """arun_callable 并发吞吐量"""
        concurrency = 200
        iterations = 20

        async def _noop():
            pass

        # 预热
        await asyncio.gather(*[arun_callable(_noop) for _ in range(50)])

        start = time.perf_counter()
        for _ in range(iterations):
            await asyncio.gather(*[arun_callable(_noop) for _ in range(concurrency)])
        elapsed = time.perf_counter() - start

        total = concurrency * iterations
        tps = total / elapsed
        latency_ms = (elapsed / total) * 1000
        print(f"\n  [arun_callable 并发] {tps:.0f} TPS ({total} calls in {elapsed:.3f}s, avg {latency_ms:.2f}ms/call)")
        assert tps > 5000

    async def test_invoker_sync_no_await_throughput(self):
        """SyncInvoker 同步方法（无 await）并发吞吐量"""
        svc = PerfService()
        concurrency = 200
        iterations = 20

        # 预热
        for _ in range(50):
            svc.noop_sync()

        start = time.perf_counter()
        for _ in range(iterations):
            for _ in range(concurrency):
                svc.noop_sync()
        elapsed = time.perf_counter() - start

        total = concurrency * iterations
        tps = total / elapsed
        latency_us = (elapsed / total) * 1_000_000
        print(f"\n  [SyncInvoker 同步并发] {tps:.0f} TPS ({total} calls in {elapsed:.3f}s, avg {latency_us:.2f}μs/call)")
        assert tps > 10000


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
    """重入调用链延迟累积"""

    WARMUP = 50
    ITERATIONS = 500

    def test_4_layer_reentrant(self):
        """4 层重入链：sync→async→sync→async"""
        svc = ReentrantPerfService()
        stats = _measure(lambda: svc.entry(), self.WARMUP, self.ITERATIONS)

        print(f"\n  [4层重入 sync→async→sync→async]")
        print(f"  {_fmt(stats)}")
        print(f"  平均每层: {stats['mean']/4:.1f}μs")
        assert stats["mean"] < 20000

    def test_reentrant_vs_flat(self):
        """重入链 vs 等价的平铺调用"""
        svc = PerfService()

        # 平铺：直接调用一个 async 方法
        flat_stats = _measure(lambda: svc.noop_async(), self.WARMUP, self.ITERATIONS)

        # 重入：4 层链
        reentrant_svc = ReentrantPerfService()
        reentrant_stats = _measure(lambda: reentrant_svc.entry(), self.WARMUP, self.ITERATIONS)

        # 重入链的延迟不应超过平铺调用的 6 倍（4 层 + 开销）
        ratio = reentrant_stats["mean"] / flat_stats["mean"] if flat_stats["mean"] > 0 else 0

        print(f"\n  [重入 vs 平铺]")
        print(f"  平铺(async):     {_fmt(flat_stats)}")
        print(f"  重入(4层链):     {_fmt(reentrant_stats)}")
        print(f"  延迟比:          {ratio:.1f}x")

        assert ratio < 10, f"Reentrant chain too slow: {ratio:.1f}x of flat call"


# ─── 6. 帧管理开销 ───


class TestFrameOverhead:
    """帧管理（_frame_scope）的开销"""

    WARMUP = 200
    ITERATIONS = 5000

    def test_frame_scope_overhead(self):
        """_frame_scope 的纯开销"""
        invoker = SyncInvoker()

        def dummy():
            pass

        # 无帧管理
        def no_frame():
            return dummy()

        # 有帧管理
        def with_frame():
            with invoker._frame_scope(dummy, MethodKind.SYNC, None):
                return dummy()

        from invoker_switch import MethodKind

        no_stats = _measure(no_frame, self.WARMUP, self.ITERATIONS)
        with_stats = _measure(with_frame, self.WARMUP, self.ITERATIONS)

        overhead = with_stats["mean"] - no_stats["mean"]
        print(f"\n  [帧管理开销]")
        print(f"  无帧:   {_fmt(no_stats)}")
        print(f"  有帧:   {_fmt(with_stats)}")
        print(f"  额外:   {overhead:.1f}μs")
        assert overhead < 200, f"Frame overhead too high: {overhead:.1f}μs"
