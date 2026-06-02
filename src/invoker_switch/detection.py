"""字节码级 await 检测 — 通过 CPython 字节码判断调用者是否使用了 await"""

import dis
import logging
import sys
from typing_extensions import Any, Dict, List

logger = logging.getLogger(__name__)

# 框架内部模块名前缀 — 用于栈帧归属判断，跳过框架内部帧
_PACKAGE_PREFIX: str = "invoker_switch."

# 字节码指令缓存 — 每个 code object 只解析一次
# key 为 id(code)，value 为 (code_id, instructions) 元组
# code_id 用于检测 id 复用导致的缓存碰撞
_instruction_cache: Dict[int, tuple[int, List[Any]]] = {}

# 缓存容量上限 — 防止极端情况下无限增长
_CACHE_MAX_SIZE: int = 1024


def _find_caller_frame() -> Any:
    """从当前栈帧向上查找，跳过所有框架内部帧，返回用户代码帧

    栈帧遍历规则：
      - 跳过 is_awaited 自身（frame 0）
      - 跳过 invoke 自身（frame 1）
      - 跳过所有属于 invoker_switch 包的帧（wrapper、_submit_coro 内部的回调等）
      - 返回第一个不属于框架的帧

    优势：不依赖函数名字符串匹配，即使框架内部增加了新的中间调用层，
    或用户代码中恰好有叫 "wrapper" 的函数，都不会误判。
    """
    # 从 frame 2 开始（跳过 is_awaited 和 invoke）
    depth = 2
    while True:
        try:
            frame = sys._getframe(depth)
        except ValueError:
            # 栈帧不够深 — 没有用户代码帧
            return None

        # 检查帧的模块归属
        module: str = frame.f_globals.get("__name__", "")
        if not module.startswith(_PACKAGE_PREFIX):
            # 不属于框架内部 → 这就是用户代码帧
            return frame

        # 属于框架内部 → 继续向上查找
        depth += 1


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
