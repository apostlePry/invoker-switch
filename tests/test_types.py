"""辅助类型模块测试 — MethodKind, CallFrame"""

import pytest

from invoker_switch import CallFrame, MethodKind


class TestMethodKind:
    """MethodKind 枚举测试"""

    def test_values(self):
        assert MethodKind.SYNC == "sync"
        assert MethodKind.ASYNC == "async"
        assert MethodKind.COROUTINE == "coroutine"

    def test_is_str_enum(self):
        """MethodKind 应为 StrEnum，可直接当字符串使用"""
        assert isinstance(MethodKind.SYNC, str)
        assert MethodKind.SYNC == "sync"


class TestCallFrame:
    """CallFrame 调用栈帧测试"""

    def test_basic_frame(self):
        frame = CallFrame(
            method_name="UserService.get_user",
            method_kind=MethodKind.SYNC,
        )
        assert frame.method_name == "UserService.get_user"
        assert frame.method_kind == MethodKind.SYNC
        assert frame.caller is None
        assert frame.caller_kind is None

    def test_frame_with_caller(self):
        caller = CallFrame(
            method_name="UserService.list_users",
            method_kind=MethodKind.ASYNC,
        )
        frame = CallFrame(
            method_name="UserService.get_user",
            method_kind=MethodKind.SYNC,
            caller=caller,
        )
        assert frame.caller is caller
        assert frame.caller_kind == MethodKind.ASYNC

    def test_nested_callers(self):
        """深层嵌套的调用链"""
        frame1 = CallFrame(method_name="a", method_kind=MethodKind.SYNC)
        frame2 = CallFrame(method_name="b", method_kind=MethodKind.ASYNC, caller=frame1)
        frame3 = CallFrame(method_name="c", method_kind=MethodKind.SYNC, caller=frame2)

        assert frame3.caller is frame2
        assert frame3.caller_kind == MethodKind.ASYNC
        assert frame2.caller is frame1
        assert frame2.caller_kind == MethodKind.SYNC
        assert frame1.caller is None
        assert frame1.caller_kind is None

    def test_frame_is_pydantic_model(self):
        """CallFrame 是 Pydantic BaseModel，支持数据验证"""
        frame = CallFrame(method_name="test", method_kind=MethodKind.SYNC)
        # 应支持 model_dump
        data = frame.model_dump()
        assert data["method_name"] == "test"
        assert data["method_kind"] == "sync"
