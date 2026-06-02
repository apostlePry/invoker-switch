"""SyncInvoker — 统一同步/异步执行器"""

from .invoker import SyncInvoker
from .loop import EventLoopManager
from .meta import InvokerBase, InvokerMeta, _invoker
from .types import CallFrame, MethodKind
from .utils import arun_callable, run_callable, smart_call

# 兼容旧名称（已弃用，将在未来版本移除）
RpcMeta = InvokerMeta
RpcBase = InvokerBase

__all__ = [
    # 核心
    "SyncInvoker",
    "InvokerBase",
    "InvokerMeta",
    # 类型
    "MethodKind",
    "CallFrame",
    # 基础设施
    "EventLoopManager",
    # 工具
    "run_callable",
    "arun_callable",
    "smart_call",
    # 兼容旧名称
    "RpcMeta",
    "RpcBase",
]
