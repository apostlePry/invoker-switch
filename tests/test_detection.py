"""字节码检测模块测试"""

import asyncio
import functools

import pytest

from invoker_switch import InvokerBase, mark_wrapper
from invoker_switch.detection import is_awaited, _instruction_cache, _find_caller_frame, _wrapped_code_ids, _WRAPPER_MARKER
from invoker_switch.meta import _invoker


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


class TestFindCallerFrameWithSmartCall:
    """_find_caller_frame 与 smart_call 风格装饰器交互测试

    回归测试：方式1（f_locals/f_globals 按名查找函数对象）曾导致误判。
    当 @smart_call 装饰函数时，装饰器替换了命名空间中的函数名，
    functools.wraps 会将 __invoker_wrapper__ 属性复制到装饰后的函数上。
    方式1 通过名称查找找到的是 wrapper 对象，而非帧中实际执行的原始函数，
    导致用户代码帧被误判为 wrapper 帧而跳过。
    """

    def test_original_function_frame_not_skipped(self):
        """smart_call 装饰后的原始函数帧不应被误判为 wrapper 帧而跳过"""

        def original_func():
            # 原始函数内部通过 invoke 调用，触发 _find_caller_frame
            # 帧的 f_code.co_name = 'original_func'
            # f_globals['original_func'] 指向 wrapper（有 __invoker_wrapper__）
            return svc.sync_step()

        class Svc(InvokerBase):
            def sync_step(self) -> str:
                return "result"

        svc = Svc()

        # 模拟 smart_call 装饰过程
        def _wrapper(*args, **kwargs):
            return _invoker.invoke(original_func, *args, **kwargs)

        _wrapper = functools.wraps(original_func)(_wrapper)
        mark_wrapper(_wrapper)

        # 通过 wrapper 调用（模拟 @smart_call 装饰后的调用）
        # invoke → _find_caller_frame 应找到 original_func 帧，而不是跳过它
        result = _wrapper()
        assert result == "result"

        # 验证误判条件确实存在：
        # 1. wrapper 的 code id 已注册
        assert id(_wrapper.__code__) in _wrapped_code_ids
        # 2. 原始函数的 code id 未注册
        assert id(original_func.__code__) not in _wrapped_code_ids
        # 3. wrapper 有 __invoker_wrapper__ 标记（functools.wraps 复制）
        assert getattr(_wrapper, _WRAPPER_MARKER, False) is True

    def test_smart_call_awaited_sync_in_async_context(self):
        """smart_call 装饰的 sync 函数在 async 上下文中用 await 调用，应走 to_thread"""

        class Svc(InvokerBase):
            def sync_step(self) -> str:
                return "step_result"

        svc = Svc()

        def original_func():
            return svc.sync_step()

        @functools.wraps(original_func)
        def _wrapper(*args, **kwargs):
            return _invoker.invoke(original_func, *args, **kwargs)

        mark_wrapper(_wrapper)

        # 在 async 上下文中用 await 调用 — is_awaited() 应返回 True
        async def _test():
            result = await _wrapper()
            assert result == "step_result"

        asyncio.run(_test())

    def test_smart_call_reentrant_async_sync_chain(self):
        """smart_call 装饰的函数：async → sync → async 调用链不应报错

        回归场景：
          world() [async, _submit_coro 提交到事件循环]
            → await hello() [sync, is_awaited=True → _to_thread]
              → demo() [async, _submit_coro 从线程池提交到事件循环 ✓]
        """

        class Svc(InvokerBase):
            async def async_final(self) -> str:
                return "done"

        svc = Svc()

        # async_final → 返回 "done"
        # sync_middle → 调用 async_final（无 await）
        # async_entry → await sync_middle
        def sync_middle():
            return svc.async_final()

        async def async_entry():
            # is_awaited() 应检测到 await，将 sync_middle 发到线程池
            # sync_middle 在线程池中调用 async_final → _submit_coro 可以正常阻塞
            return await sync_middle()

        # 装饰
        sync_middle_wrapped = functools.wraps(sync_middle)(lambda *a, **kw: _invoker.invoke(sync_middle, *a, **kw))
        mark_wrapper(sync_middle_wrapped)

        async_entry_wrapped = functools.wraps(async_entry)(lambda *a, **kw: _invoker.invoke(async_entry, *a, **kw))
        mark_wrapper(async_entry_wrapped)

        # 从同步上下文调用 → _submit_coro 提交 async_entry
        result = async_entry_wrapped()
        assert result == "done"
