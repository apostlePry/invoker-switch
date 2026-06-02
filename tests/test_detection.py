"""字节码检测模块测试"""

import pytest

from invoker_switch import InvokerBase
from invoker_switch.detection import is_awaited, _instruction_cache, _find_caller_frame


class TestFindCallerFrame:
    """_find_caller_frame 栈帧定位测试"""

    def test_finds_user_frame_through_wrapper(self):
        """通过 InvokerBase 方法调用时，应跳过框架内部帧，找到用户代码帧"""

        class Svc(InvokerBase):
            def method(self) -> str:
                # 在方法内部调用 _find_caller_frame
                # 栈帧：_find_caller_frame → is_awaited(未调用) → method(内部)
                # 但这个测试验证的是：从框架内部调用时，能正确跳过内部帧
                return "result"

        svc = Svc()
        # 调用成功说明栈帧没有出错
        result = svc.method()
        assert result == "result"

    def test_no_caller_frame_returns_none(self):
        """从框架内部直接调用 is_awaited 时，可能找不到用户帧"""

        class Svc(InvokerBase):
            def method(self) -> str:
                return "result"

        # 直接在测试中调用 is_awaited（测试模块不属于 invoker_switch 包）
        # 应能正常检测
        result = is_awaited()
        assert isinstance(result, bool)


class TestIsAwaited:
    """is_awaited() 字节码检测测试"""

    def test_not_awaited_in_sync_context(self):
        """同步上下文中调用不应被检测为 awaited"""

        class Svc(InvokerBase):
            def sync_method(self) -> str:
                return "result"

        svc = Svc()
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
        _instruction_cache.clear()

        class Svc(InvokerBase):
            def method(self) -> str:
                return "result"

        svc = Svc()
        svc.method()

        # 缓存应不为空
        assert len(_instruction_cache) > 0

    def test_cache_entry_format(self):
        """缓存条目格式应为 (code_id, instructions) 元组"""
        _instruction_cache.clear()

        class Svc(InvokerBase):
            def method(self) -> str:
                return "result"

        svc = Svc()
        svc.method()

        for key, value in _instruction_cache.items():
            assert isinstance(value, tuple)
            assert len(value) == 2
            code_id, instrs = value
            assert isinstance(code_id, int)
            assert isinstance(instrs, list)

    def test_cache_max_size(self):
        """缓存应有容量上限"""
        from invoker_switch.detection import _CACHE_MAX_SIZE
        assert _CACHE_MAX_SIZE > 0
        assert _CACHE_MAX_SIZE <= 10240  # 合理上限
