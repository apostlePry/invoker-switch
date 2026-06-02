"""字节码级 await 检测 — 通过 CPython 字节码判断调用者是否使用了 await"""

import dis
import logging
import sys
from typing_extensions import Any, Callable, Dict, List, Tuple

logger = logging.getLogger(__name__)

# 框架内部模块名前缀 — 用于栈帧归属判断，跳过框架内部帧
_PACKAGE_PREFIX: str = "invoker_switch."

# 装饰器 wrapper 标记属性名
# smart_call 装饰器生成的 wrapper 会被打上此属性，_find_caller_frame 遇到时跳过
_WRAPPER_MARKER: str = "__invoker_wrapper__"

# 字节码指令缓存 — 每个 code object 只解析一次
# key 为 id(code)，value 为 (code_id, instructions) 元组
# code_id 用于检测 id 复用导致的缓存碰撞
_instruction_cache: Dict[int, Tuple[int, List[Any]]] = {}

# 缓存容量上限 — 防止极端情况下无限增长
_CACHE_MAX_SIZE: int = 1024


def _find_caller_frame() -> Any:
    """从当前栈帧向上查找，跳过所有框架内部帧和装饰器 wrapper 帧，返回用户代码帧

    栈帧遍历规则：
      - 跳过 is_awaited 自身（frame 0）
      - 跳过 invoke 自身（frame 1）
      - 跳过所有属于 invoker_switch 包的帧（wrapper、_submit_coro 内部的回调等）
      - 跳过带 __invoker_wrapper__ 标记的装饰器 wrapper 帧
      - 返回第一个不属于框架的帧
    """
    # 从 frame 2 开始（跳过 is_awaited 和 invoke）
    depth = 2
    while True:
        try:
            frame = sys._getframe(depth)
        except ValueError:
            # 栈帧不够深 — 没有用户代码帧
            return None

        # 检查是否是装饰器 wrapper 帧（带标记的函数）
        if frame.f_code.co_flags & 0x04:  # CO_OPTIMIZED
            # wrapper 是局部函数，检查 f_locals 中是否有标记
            # 更可靠的方式：检查 f_code 的属性
            pass

        # 方式1：检查帧对应的函数对象是否有 __invoker_wrapper__ 标记
        # f_code.co_name 是函数名，但无法直接拿到函数对象
        # 所以通过 f_locals 或 f_globals 间接查找
        func_name = frame.f_code.co_name
        # 在帧的 locals 和 globals 中查找同名函数并检查标记
        func_obj = frame.f_locals.get(func_name) or frame.f_globals.get(func_name)
        if func_obj is not None and getattr(func_obj, _WRAPPER_MARKER, False):
            # 装饰器 wrapper 帧 → 跳过
            depth += 1
            continue

        # 方式2：检查帧的 f_code 上是否有标记
        # 装饰器通过 _mark_wrapper() 给 code object 设置标记
        if getattr(frame.f_code, _WRAPPER_MARKER, False):
            depth += 1
            continue

        # 检查帧的模块归属
        module: str = frame.f_globals.get("__name__", "")
        if not module.startswith(_PACKAGE_PREFIX):
            # 不属于框架内部 → 这就是用户代码帧
            return frame

        # 属于框架内部 → 继续向上查找
        depth += 1


def _mark_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
    """给装饰器 wrapper 函数打上标记，使 _find_caller_frame 能识别并跳过它

    用法：
        def smart_call(func):
            @functools.wraps(func)
            def _wrapper(*args, **kwargs):
                return _invoker.invoke(func, *args, **kwargs)
            return _mark_wrapper(_wrapper)

    标记方式：在 wrapper 的 __code__ 对象上设置 __invoker_wrapper__ = True
    """
    try:
        # code object 正常是不可变的，但可以动态设置属性（CPython 允许）
        setattr(func.__code__, _WRAPPER_MARKER, True)
    except (AttributeError, TypeError):
        # 某些实现可能不支持，降级：在函数对象上设置标记
        pass
    # 同时在函数对象上也设置标记，作为备用
    setattr(func, _WRAPPER_MARKER, True)
    return func


def is_awaited() -> bool:
    """检查调用者是否使用了 await

    通过检查用户代码栈帧的字节码，判断调用指令后面是否紧跟
    GET_AWAITABLE 指令，从而判断本次调用是否会被 await。

    返回值：
        True  — 调用者使用了 await（如 result = await obj.method()）
        False — 调用者未使用 await（如 result = obj.method()）
                或检测失败时的安全降级
    """
    try:
        caller_frame = _find_caller_frame()
        if caller_frame is None:
            return False

        code = caller_frame.f_code
        lasti = caller_frame.f_lasti

        # 使用缓存的指令列表
        cache_key = id(code)
        cached = _instruction_cache.get(cache_key)
        if cached is None or cached[0] != id(code):
            # 缓存未命中或 id 复用 — 解析并缓存
            if len(_instruction_cache) >= _CACHE_MAX_SIZE:
                _instruction_cache.clear()
            instrs = list(dis.get_instructions(code))
            _instruction_cache[cache_key] = (id(code), instrs)
        else:
            instrs = cached[1]

        for instr in instrs:
            if instr.offset > lasti:
                if instr.opname == "GET_AWAITABLE":
                    return True
                break
    except Exception:
        logger.debug("is_awaited() detection failed, defaulting to False", exc_info=True)
    return False
