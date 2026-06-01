"""字节码检测模块测试"""

import pytest

from invoker_switch import InvokerBase, WRAPPER_FUNC_NAME
from invoker_switch.detection import is_awaited, _instruction_cache


class TestWrapperFuncName:
    """WRAPPER_FUNC_NAME 常量测试"""

    def test_constant_value(self):
        assert WRAPPER_FUNC_NAME == "wrapper"

    def test_constant_type(self):
        assert isinstance(WRAPPER_FUNC_NAME, str)


class TestIsAwaited:
    """is_awaited() 字节码检测测试"""

    def test_not_awaited_in_sync_context(self):
        """同步上下文中调用不应被检测为 awaited"""

        class Svc(InvokerBase):
            def sync_method(self) -> str:
                return "result"

        svc = Svc()
        # 直接调用（无 await）不应触发 awaited 检测
        result = svc.sync_method()
        assert result == "result"

    async def test_not_awaited_in_async_context(self):
        """异步上下文中无 await 调用同步方法，应直接返回结果"""

        class Svc(InvokerBase):
            def sync_method(self) -> str:
                return "result"

        svc = Svc()
        result = svc.sync_method()
        assert result == "result"

    async def test_awaited_in_async_context(self):
        """异步上下文中有 await 调用同步方法，应通过 to_thread 执行"""

        class Svc(InvokerBase):
            def sync_method(self) -> str:
                return "result"

        svc = Svc()
        result = await svc.sync_method()
        assert result == "result"


class TestInstructionCache:
    """字节码指令缓存测试"""

    def test_cache_populated_after_call(self):
        """调用后指令缓存应被填充"""
        # 清空缓存
        _instruction_cache.clear()

        class Svc(InvokerBase):
            def method(self) -> str:
                return "result"

        svc = Svc()
        svc.method()

        # 缓存应不为空（至少缓存了调用者的字节码）
        assert len(_instruction_cache) > 0
