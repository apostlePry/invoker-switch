"""辅助类型定义 — 方法类型枚举、调用栈帧、模块级状态"""

import contextvars
from enum import Enum

from pydantic import BaseModel
from typing_extensions import List, Optional


class MethodKind(str, Enum):
    """方法类型枚举

    继承 str 和 Enum（而非 StrEnum）以兼容 Python 3.8。
    StrEnum 在 Python 3.11 才引入。
    """

    SYNC = "sync"  # 普通 def 方法
    ASYNC = "async"  # async def 方法
    COROUTINE = "coroutine"  # 协程对象（已调用但未 await）


class CallFrame(BaseModel):
    """调用栈帧 — 记录当前方法调用信息，构成链表结构"""

    method_name: str  # 方法全限定名（如 UserService.get_user）
    method_kind: MethodKind  # 当前方法类型
    caller: Optional["CallFrame"] = None  # 调用者帧（链表指针）

    @property
    def caller_kind(self) -> Optional[MethodKind]:
        """获取调用者的方法类型"""
        if self.caller is None:
            return None
        return self.caller.method_kind


# ─── 模块级状态 ───

_call_stack: contextvars.ContextVar[List[CallFrame]] = contextvars.ContextVar(
    "_call_stack",
    default=[],
)
