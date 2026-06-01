"""重入场景测试 — 同步→异步→同步 交替调用"""

import pytest

from invoker_switch import InvokerBase


class ReentrantService(InvokerBase):
    """重入服务 — 同步→异步→同步 交替调用"""

    def entry(self) -> str:
        """同步入口方法"""
        return self.async_step()

    async def async_step(self) -> str:
        """异步中间方法"""
        r = await self.sync_step()
        return f"async->{r}"

    def sync_step(self) -> str:
        """同步中间方法"""
        return self.async_final()

    async def async_final(self) -> str:
        """异步最终方法"""
        return "done"

    # 变体：带参数的重入
    def compute_entry(self, x: int) -> str:
        return self.compute_async(x)

    async def compute_async(self, x: int) -> str:
        r = await self.compute_sync(x)
        return f"result={r}"

    def compute_sync(self, x: int) -> str:
        return self.compute_async_final(x)

    async def compute_async_final(self, x: int) -> str:
        return str(x * 2)


class TestReentrant:
    """重入场景：同步→异步→同步→异步"""

    def test_basic_reentrant(self):
        """同步入口 → 异步 → 同步 → 异步：entry() → async_step() → sync_step() → async_final()"""
        svc = ReentrantService()
        result = svc.entry()
        assert result == "async->done"

    def test_reentrant_with_args(self):
        """带参数的重入调用"""
        svc = ReentrantService()
        result = svc.compute_entry(5)
        assert result == "result=10"

    async def test_reentrant_from_async(self):
        """从异步入口开始的重入调用"""
        svc = ReentrantService()
        result = await svc.async_step()
        assert result == "async->done"

    async def test_reentrant_from_async_with_args(self):
        """从异步入口开始的重入调用（带参数）"""
        svc = ReentrantService()
        result = await svc.compute_async(7)
        assert result == "result=14"
