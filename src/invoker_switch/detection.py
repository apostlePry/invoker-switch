"""字节码级 await 检测 — 通过 CPython 字节码判断调用者是否使用了 await"""

import dis
import sys
from typing_extensions import Any, Dict, List

# wrapper 函数名常量 — 供 InvokerMeta._wrap_method 生成的 wrapper 和
# _is_awaited() 共同引用，避免隐式耦合
WRAPPER_FUNC_NAME: str = "wrapper"

# 字节码指令缓存 — 每个 code object 只解析一次
_instruction_cache: Dict[int, List[Any]] = {}


def is_awaited() -> bool:
    """检查调用者是否使用了 await

    通过检查调用栈帧的字节码，判断调用指令后面是否紧跟 GET_AWAITABLE 指令。

    栈帧关系：
        调用方式 1 — 通过 InvokerBase 子类方法：
          frame 3: user_code         ← result = await obj.method()
          frame 2: wrapper           ← _invoker.invoke(func, self, *args)
          frame 1: invoke            ← is_awaited()
          frame 0: is_awaited

        调用方式 2 — 直接调用 invoker.invoke：
          frame 2: user_code         ← result = await invoker.invoke(func)
          frame 1: invoke            ← is_awaited()
          frame 0: is_awaited
    """
    try:
        # frame 0: is_awaited
        # frame 1: invoke
        frame = sys._getframe(1)

        # 检查 frame 2 是否是 InvokerMeta 的 wrapper
        frame2 = sys._getframe(2)
        frame2_name = frame2.f_code.co_name

        if frame2_name == WRAPPER_FUNC_NAME:
            # 通过 InvokerBase 方法调用，实际调用者在 frame 3
            caller_frame = sys._getframe(3)
        else:
            # 直接调用 invoker.invoke，实际调用者在 frame 2
            caller_frame = frame2

        code = caller_frame.f_code
        lasti = caller_frame.f_lasti

        # 使用缓存的指令列表
        cache_key = id(code)
        if cache_key not in _instruction_cache:
            _instruction_cache[cache_key] = list(dis.get_instructions(code))
        instrs = _instruction_cache[cache_key]

        for instr in instrs:
            if instr.offset > lasti:
                if instr.opname == "GET_AWAITABLE":
                    return True
                break
    except Exception:
        pass
    return False
