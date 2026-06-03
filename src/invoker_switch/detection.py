"""字节码级 await 检测 — 通过 CPython 字节码判断调用者是否使用了 await"""

import dis
import logging
import sys
from typing_extensions import Any, Callable, Dict, List, Set, Tuple

logger = logging.getLogger(__name__)

# 框架内部模块名前缀 — 用于栈帧归属判断，跳过框架内部帧
_PACKAGE_PREFIX: str = "invoker_switch."

# 装饰器 wrapper 标记属性名
# smart_call 装饰器生成的 wrapper 会被打上此属性，_find_caller_frame 遇到时跳过
_WRAPPER_MARKER: str = "__invoker_wrapper__"

# 已标记的 wrapper code object id 集合
# mark_wrapper() 注册，_find_caller_frame() 查询，用于可靠跳过 wrapper 帧
# 解决的问题：
#   1. Python 3.12+ code object 不允许 setattr，方式2（f_code 属性检测）失效
#   2. wrapper 是闭包局部函数，f_locals/f_globals 按名查找找不到
#   3. wrapper 定义在 invoker_switch 包外，模块归属检测不跳过
# 只要函数存活（被装饰后持有引用），id(code) 就不会复用，查找可靠
_wrapped_code_ids: Set[int] = set()

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
      - 跳过 mark_wrapper 注册的装饰器 wrapper 帧（通过 code object id 检测）
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

        # 方式0（最可靠）：检查 code object id 是否在已注册的 wrapper 集合中
        # 适用于所有 Python 版本，不受 code object 不可变限制影响
        if id(frame.f_code) in _wrapped_code_ids:
            depth += 1
            continue

        # 方式2：检查帧的 f_code 上是否有标记
        # 仅在 Python <3.12（code object 允许 setattr）时有效
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


def mark_wrapper(func: Callable[..., Any]) -> Callable[..., Any]:
    """给装饰器 wrapper 函数打上标记，使 _find_caller_frame 能识别并跳过它

    用法：
        def smart_call(func):
            @functools.wraps(func)
            def _wrapper(*args, **kwargs):
                return _invoker.invoke(func, *args, **kwargs)
            return mark_wrapper(_wrapper)

    标记方式（双重保障）：
      1. code object id 注册到 _wrapped_code_ids 集合（最可靠，所有版本通用）
      2. 在 wrapper 的 __code__ 对象上设置属性（Python <3.12 可用）

    注意：不再通过函数对象属性 (__invoker_wrapper__) 进行帧检测。
          该属性仍会被设置以保持向后兼容，但 _find_caller_frame 不再使用它，
          因为 f_locals/f_globals 按名查找可能找到装饰后的 wrapper 函数而非帧中
          实际执行的原始函数，导致用户代码帧被误判为 wrapper 帧而跳过。
    """
    # 方式0：注册 code object id（最可靠，_find_caller_frame 优先使用）
    _wrapped_code_ids.add(id(func.__code__))

    try:
        # 方式2：code object 属性标记（Python <3.12 可用，3.12+ 静默失败）
        setattr(func.__code__, _WRAPPER_MARKER, True)
    except (AttributeError, TypeError):
        pass
    # 保留函数对象属性标记（向后兼容：外部代码可能检查此属性）
    # 注意：_find_caller_frame 不再使用此属性进行帧检测
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
