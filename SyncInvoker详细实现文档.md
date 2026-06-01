# SyncInvoker 详细实现文档

## 1. 问题定义：为什么需要 SyncInvoker

Python 的 async/await 模型存在一个根本性限制：**同步代码和异步代码不能直接互调**。

```
❌ 同步函数中直接调用异步函数 → 得到协程对象，而非结果
❌ 异步函数中直接调用阻塞的同步函数 → 阻塞事件循环，所有协程卡死
❌ 同步→异步→同步 交替调用 → 事件循环线程死锁
```

在 RPC 框架中，这个问题尤为突出：

- **Consumer 端**：用户在 Django 视图（同步）中调用远程服务（异步网络 I/O）
- **Provider 端**：用户实现的是同步业务方法，框架需要异步网络 I/O 返回响应
- **混合场景**：同步方法内部调用异步数据库驱动，异步方法内部调用同步缓存

**SyncInvoker 的目标**：让用户完全不需要关心同步/异步边界，像写普通 Python 代码一样调用任何方法，框架自动处理执行策略。

---

## 2. 设计目标与约束

### 2.1 设计目标

| 目标 | 描述 |
|------|------|
| 透明调用 | 同步/异步方法统一调用入口，结果类型一致 |
| 上下文感知 | 根据当前执行上下文（同步/异步）自动选择最优执行策略 |
| await 感知 | 区分 `obj.method()` 和 `await obj.method()` 的不同语义 |
| 重入安全 | 同步→异步→同步 交替调用不发生死锁 |
| 上下文传播 | ContextVar 在跨线程/跨事件循环调用中正确传播 |
| 双循环模式 | 支持外部注入事件循环（FastAPI）和内置事件循环（独立使用） |

### 2.2 约束

| 约束 | 说明 |
|------|------|
| 不能阻塞事件循环 | 在异步上下文中，同步方法必须卸载到线程池 |
| 不能在同一线程死锁 | 事件循环线程内提交协程到自身会死锁 |
| 字节码兼容性 | `_is_awaited()` 依赖 CPython 字节码细节 |
| 全局单例 | `_invoker` 和 `EventLoopManager` 是全局状态 |

---

## 3. 完整类结构

```
invoker.py
│
├── 枚举与数据类
│   ├── MethodKind(StrEnum)           方法类型：SYNC / ASYNC / COROUTINE
│   └── CallFrame(BaseModel)          调用栈帧：method_name + method_kind + caller
│
├── 模块级状态
│   ├── _call_stack: ContextVar       调用栈（ContextVar 隔离）
│   └── _instruction_cache: Dict      字节码指令缓存
│
├── 模块级函数
│   ├── _is_awaited() -> bool         字节码级 await 检测
│   └── _wrap_method() -> wrapper     方法包装器（RpcMeta 使用）
│
├── EventLoopManager                  事件循环管理器（SyncInvoker 的基础设施）
│
├── SyncInvoker                       核心执行器
│   ├── 属性
│   │   └── current_frame             当前调用栈顶帧
│   ├── 上下文判断
│   │   ├── _get_method_kind()        判断方法类型
│   │   └── _is_in_async_context()    判断是否在异步上下文
│   ├── 调用栈管理
│   │   ├── _push_frame()             压入栈帧
│   │   └── _pop_frame()              弹出栈帧
│   ├── 直接执行（异步上下文中）
│   │   ├── _execute_sync()           直接执行同步方法
│   │   ├── _execute_sync_as_coro()   包装同步方法为协程（to_thread）
│   │   └── _execute_async()          await 执行异步方法
│   ├── 提交执行（同步上下文中，阻塞等待结果）
│   │   ├── _submit_sync()            提交同步方法到线程池
│   │   └── _submit_coro()            提交协程到事件循环
│   ├── 辅助方法
│   │   └── _run_coro_with_context()  在指定循环中运行协程并传播上下文
│   └── 统一入口
│       └── invoke()                  自动判断 + 分发
│
├── RpcMeta(ABCMeta)                  元类：拦截类创建，包装所有方法
├── RpcBase(metaclass=RpcMeta)        基类：子类方法自动转发给 SyncInvoker
└── run_callable()                    轻量级异步执行工具
```

---

## 4. 逐步实现详解

### 步骤一：定义方法类型枚举与调用栈帧

这是整个系统的基石——需要一种方式来标记和追踪每个方法的类型。

```python
from enum import StrEnum
from pydantic import BaseModel
from typing_extensions import Optional


class MethodKind(StrEnum):
    """方法类型枚举"""
    SYNC = "sync"           # 普通 def 方法
    ASYNC = "async"         # async def 方法
    COROUTINE = "coroutine" # 协程对象（已调用但未 await）


class CallFrame(BaseModel):
    """调用栈帧 — 记录当前方法调用信息，构成链表结构"""
    method_name: str                         # 方法全限定名（如 UserService.get_user）
    method_kind: MethodKind                  # 当前方法类型
    caller: Optional["CallFrame"] = None     # 调用者帧（链表指针）

    @property
    def caller_kind(self) -> Optional[MethodKind]:
        """获取调用者的方法类型"""
        if self.caller is None:
            return None
        return self.caller.method_kind
```

**设计决策**：

| 决策 | 原因 |
|------|------|
| `CallFrame` 使用 Pydantic BaseModel | 数据验证、不可变性、序列化支持 |
| `caller` 链表结构 | O(1) 访问直接调用者，支持任意深度追溯 |
| `MethodKind` 使用 StrEnum | 可读性好，序列化友好 |

**调用栈基于 ContextVar**：

```python
import contextvars

_call_stack: contextvars.ContextVar[list[CallFrame]] = contextvars.ContextVar(
    "_call_stack",
    default=[],
)
```

为什么不用 `threading.local()`？因为 `ContextVar` 天然支持 asyncio 任务隔离——每个 Task 有独立的上下文副本，不会在协程间串栈。

---

### 步骤二：实现字节码级 await 检测

这是 SyncInvoker 最精妙的部分——需要判断调用者是否使用了 `await` 来接收返回值。

**为什么需要检测 await？**

```python
class Service(RpcBase):
    def sync_method(self) -> str:
        return "hello"

    async def async_caller(self) -> str:
        # 场景 1：没有 await → 期望直接得到结果字符串
        result = self.sync_method()

        # 场景 2：有 await → 期望得到协程，在线程池中执行
        result = await self.sync_method()
```

同一个同步方法，在同一个异步上下文中，`await` 与否导致完全不同的执行策略。

**实现原理**：

```python
import dis
import sys

_instruction_cache: dict[int, list[Any]] = {}


def _is_awaited() -> bool:
    """检查调用者是否使用了 await

    通过检查调用栈帧的字节码，判断调用指令后面是否紧跟 GET_AWAITABLE 指令。
    """
    try:
        # frame 0: _is_awaited
        # frame 1: invoke
        frame = sys._getframe(1)

        # 检查 frame 2 是否是 wrapper
        frame2 = sys._getframe(2)
        frame2_name = frame2.f_code.co_name

        if frame2_name == "wrapper":
            # 通过 RpcBase 方法调用，实际调用者在 frame 3
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
```

**栈帧关系图**：

```
调用方式 1：通过 RpcBase 子类方法
  frame 3: user_code         ← result = await obj.method()   (GET_AWAITABLE 在这里)
  frame 2: wrapper           ← _invoker.invoke(func, self, *args)
  frame 1: invoke            ← _is_awaited()
  frame 0: _is_awaited

调用方式 2：直接调用 invoker.invoke
  frame 2: user_code         ← result = await invoker.invoke(func)
  frame 1: invoke            ← _is_awaited()
  frame 0: _is_awaited
```

**字节码示例**：

```python
# result = await obj.method()
#
# 编译后的字节码片段：
# ...
# CALL                 # 调用 obj.method()
# GET_AWAITABLE        ← 紧接着就是 GET_AWAITABLE → 说明被 await 了
# LOAD_CONST           # 加载 await 的 None 参数
# ...

# result = obj.method()
#
# 编译后的字节码片段：
# ...
# CALL                 # 调用 obj.method()
# STORE_NAME           ← 紧接着是 STORE_NAME → 说明没被 await
# ...
```

**缓存优化**：每个 `code object` 的字节码只需解析一次，后续从 `_instruction_cache` 读取。

---

### 步骤三：实现 EventLoopManager

SyncInvoker 需要事件循环来执行协程，需要线程池来执行同步方法。EventLoopManager 封装了这两种资源的获取逻辑。

```python
import asyncio
import concurrent.futures
import threading


class EventLoopManager:
    """事件循环管理器 — 双模式架构"""

    # 外部注入模式的状态
    _external_loop: Optional[asyncio.AbstractEventLoop] = None
    _external_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None

    # 内置模式的状态
    _internal_loop: Optional[asyncio.AbstractEventLoop] = None
    _internal_thread: Optional[threading.Thread] = None
    _internal_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    _started: threading.Event = threading.Event()
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def set_event_loop(
        cls,
        loop: asyncio.AbstractEventLoop,
        executor: Optional[concurrent.futures.ThreadPoolExecutor] = None,
    ) -> None:
        """注入外部事件循环（如 FastAPI 的）"""
        cls._external_loop = loop
        cls._external_executor = executor

    @classmethod
    def clear_event_loop(cls) -> None:
        """清除外部循环，切回内置模式"""
        cls._external_loop = None
        cls._external_executor = None

    @classmethod
    def get_event_loop(cls) -> asyncio.AbstractEventLoop:
        """获取事件循环：优先外部，否则内置"""
        if cls._external_loop is not None:
            return cls._external_loop
        return cls._ensure_internal_loop()

    @classmethod
    def get_executor(cls) -> concurrent.futures.ThreadPoolExecutor:
        """获取线程池：优先外部，否则内置"""
        if cls._external_executor is not None:
            return cls._external_executor
        return cls._ensure_internal_executor()

    @classmethod
    def _ensure_internal_loop(cls) -> asyncio.AbstractEventLoop:
        """双重检查锁定创建内置事件循环"""
        if cls._internal_loop is not None:
            if cls._internal_loop.is_closed():
                cls._internal_loop = None
                cls._internal_thread = None
            else:
                return cls._internal_loop

        with cls._lock:
            if cls._internal_loop is not None:
                if cls._internal_loop.is_closed():
                    cls._internal_loop = None
                    cls._internal_thread = None
                else:
                    return cls._internal_loop

            # 尝试获取当前运行的事件循环
            try:
                cls._internal_loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            # 没有运行中的循环，创建后台循环
            if cls._internal_loop is None:
                cls._started = threading.Event()
                cls._internal_loop = asyncio.new_event_loop()
                cls._internal_thread = threading.Thread(
                    target=cls._run_internal_loop,
                    daemon=True,
                    name="rpc-bg-loop",
                )
                cls._internal_thread.start()
                cls._started.wait()  # 等待循环启动
            return cls._internal_loop

    @classmethod
    def _run_internal_loop(cls) -> None:
        """在后台线程中运行内置事件循环"""
        loop = cls._internal_loop
        assert loop is not None
        asyncio.set_event_loop(loop)
        cls._started.set()  # 通知主线程循环已就绪
        loop.run_forever()

    @classmethod
    def _ensure_internal_executor(cls) -> concurrent.futures.ThreadPoolExecutor:
        """双重检查锁定创建内置线程池"""
        if cls._internal_executor is not None:
            return cls._internal_executor

        with cls._lock:
            if cls._internal_executor is not None:
                return cls._internal_executor

            cls._internal_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=10,
                thread_name_prefix="rpc-worker",
            )
            return cls._internal_executor

    @classmethod
    def shutdown(cls) -> None:
        """关闭内置资源"""
        if cls._internal_loop is not None:
            cls._internal_loop.call_soon_threadsafe(cls._internal_loop.stop)
            if cls._internal_thread is not None:
                cls._internal_thread.join(timeout=5)
            cls._internal_loop = None
            cls._internal_thread = None

        if cls._internal_executor is not None:
            cls._internal_executor.shutdown(wait=False)
            cls._internal_executor = None
```

**创建流程图**：

```
get_event_loop()
│
├─ _external_loop 不为 None?
│   └─ YES → 返回外部循环
│
└─ _ensure_internal_loop()
    │
    ├─ 快速路径：_internal_loop 存在且未关闭?
    │   └─ YES → 返回
    │
    └─ 获取锁（双重检查锁定）
        │
        ├─ 再次检查 _internal_loop
        │
        ├─ 尝试 asyncio.get_running_loop()
        │   └─ 成功 → 使用当前循环
        │
        └─ 创建后台循环
            ├─ new_event_loop()
            ├─ Thread(daemon=True, target=run_forever)
            ├─ start()
            └─ _started.wait() → 确保循环已就绪
```

**为什么用双重检查锁定**？

- 快速路径（无锁）：已创建的循环直接返回，避免每次获取锁
- 慢速路径（加锁）：首次创建时保证线程安全
- 防止多个线程同时创建事件循环

---

### 步骤四：实现 SyncInvoker 核心方法

#### 4.1 方法类型判断

```python
class SyncInvoker:

    def _get_method_kind(self, func: Callable[..., Any]) -> str:
        """判断方法类型"""
        # 检查 __wrapped__：RpcMeta 包装后的方法保留了原始方法引用
        original_func = getattr(func, "__wrapped__", func)

        if asyncio.iscoroutinefunction(original_func) or asyncio.iscoroutinefunction(func):
            return MethodKind.ASYNC

        if inspect.iscoroutine(func):
            return MethodKind.COROUTINE

        return MethodKind.SYNC
```

**为什么需要解包 `__wrapped__`？**

RpcMeta 的 `_wrap_method` 生成的是同步 wrapper，如果不解包，所有方法都会被判断为 SYNC：

```python
# RpcMeta 包装后的方法
def wrapper(self, *args, **kwargs):
    return _invoker.invoke(func, self, *args, **kwargs)

# wrapper 本身是同步函数 → asyncio.iscoroutinefunction(wrapper) == False
# 但 wrapper.__wrapped__ 是原始的 async def → asyncio.iscoroutinefunction(__wrapped__) == True
```

#### 4.2 异步上下文判断

```python
    def _is_in_async_context(self) -> bool:
        """检查是否在异步上下文中

        判断条件：有运行中的事件循环 且 调用者是异步方法
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False  # 没有运行中的事件循环 → 同步上下文

        caller = self.current_frame
        # caller 为 None 表示入口调用（在 async def main 中直接调用）
        # caller 为 ASYNC 表示当前方法被异步方法调用
        return caller is None or caller.method_kind == MethodKind.ASYNC
```

**为什么不能仅靠 `get_running_loop()` 判断？**

因为 SyncInvoker 在同步上下文中也会通过 `_submit_coro` 提交协程到事件循环。提交后，协程在事件循环线程中执行，此时 `get_running_loop()` 返回 True，但调用发起者仍然是同步代码。需要结合调用栈帧来区分。

#### 4.3 调用栈管理

```python
    @property
    def current_frame(self) -> Optional[CallFrame]:
        """获取当前调用栈顶帧"""
        stack = _call_stack.get()
        if not stack:
            return None
        return stack[-1]

    def _push_frame(
        self,
        func: Callable[..., Any],
        method_kind: MethodKind,
        caller: Optional[CallFrame],
    ) -> contextvars.Token:
        """压入调用栈帧，返回 token 用于恢复"""
        frame = CallFrame(
            method_name=func.__qualname__,
            method_kind=method_kind,
            caller=caller,
        )
        stack = _call_stack.get().copy()  # 复制，避免修改原列表
        stack.append(frame)
        return _call_stack.set(stack)      # 返回 token

    def _pop_frame(self, token: contextvars.Token) -> None:
        """弹出调用栈帧（恢复到 push 之前的状态）"""
        _call_stack.reset(token)
```

**为什么要用 `Token` 模式？**

```python
# 错误做法：直接 pop
stack = _call_stack.get()
stack.pop()         # 如果方法抛异常，pop 不会执行 → 栈泄漏

# 正确做法：Token + finally
token = _call_stack.set(new_stack)
try:
    return func(*args, **kwargs)
finally:
    _call_stack.reset(token)  # 无论是否异常，都能恢复
```

---

### 步骤五：实现三种直接执行方法（异步上下文中使用）

这三个方法在**异步上下文**中被调用，不需要阻塞等待。

```python
    def _execute_sync(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """直接执行同步方法，返回结果

        适用场景：异步上下文 + 同步方法 + 无 await
        """
        token = self._push_frame(func, MethodKind.SYNC, caller)
        try:
            return func(*args, **kwargs)
        finally:
            self._pop_frame(token)

    async def _execute_sync_as_coro(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """将同步方法包装为协程执行（通过 asyncio.to_thread）

        适用场景：异步上下文 + 同步方法 + 有 await
        """
        ctx = contextvars.copy_context()

        def run_in_thread():
            return self._execute_sync(func, args, kwargs, caller)

        return await asyncio.to_thread(ctx.run, run_in_thread)

    async def _execute_async(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """await 执行异步方法，返回协程

        适用场景：异步上下文 + 异步方法
        """
        token = self._push_frame(func, MethodKind.ASYNC, caller)
        try:
            return await func(*args, **kwargs)
        finally:
            self._pop_frame(token)
```

**`_execute_sync_as_coro` 的上下文传播**：

```python
ctx = contextvars.copy_context()

def run_in_thread():
    return self._execute_sync(func, args, kwargs, caller)

return await asyncio.to_thread(ctx.run, run_in_thread)
```

为什么用 `ctx.run(run_in_thread)` 而不是直接 `asyncio.to_thread(func, *args)`？

因为 `asyncio.to_thread` 默认会复制当前上下文，但我们需要确保 `_push_frame` 设置的 ContextVar 在新线程中可见。通过 `copy_context().run()` 显式控制上下文。

---

### 步骤六：实现两种提交执行方法（同步上下文中使用）

这两个方法在**同步上下文**中被调用，需要**阻塞等待结果**。

```python
    def _submit_sync(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """提交同步方法到线程池，同步等待结果

        适用场景：同步上下文 + 同步方法
        """
        executor = EventLoopManager.get_executor()
        future = executor.submit(
            self._execute_sync, func, args, kwargs, caller
        )
        return future.result()  # 阻塞等待
```

#### 6.1 `_submit_coro` — 最复杂的方法

这是整个 SyncInvoker 中最复杂的方法，需要处理**重入死锁**问题。

**死锁场景**：

```
同步方法 a() 调用异步方法 b() → _submit_coro 提交 b 到事件循环
→ b() 在事件循环线程中执行
→ b() 调用同步方法 c() → _submit_coro 需要提交 c 到事件循环
→ 但事件循环线程正在执行 b，_submit_coro 的 future.result() 阻塞等待
→ 事件循环被 b 占用，无法执行 c → 死锁！
```

```python
    def _submit_coro(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional[CallFrame],
    ) -> Any:
        """提交协程到事件循环，同步等待结果（带重入死锁防护）"""
        loop = EventLoopManager.get_event_loop()
        ctx = contextvars.copy_context()

        # 检测是否在事件循环线程中
        try:
            running_loop = asyncio.get_running_loop()
            in_loop_thread = (running_loop is loop)
        except RuntimeError:
            in_loop_thread = False

        if in_loop_thread:
            # ─── 在事件循环线程中 → 不能阻塞等待当前循环 ───

            # 策略 1：尝试使用内置事件循环（如果存在且不是当前循环）
            internal_loop = EventLoopManager._internal_loop
            if internal_loop is not None and internal_loop is not running_loop:
                result, new_ctx = self._run_coro_with_context(
                    internal_loop, func, args, kwargs, caller, ctx
                )
                # 应用上下文变更到当前线程
                for var in new_ctx.keys():
                    var.set(new_ctx[var])
                return result

            # 策略 2：在线程池中创建新循环执行
            executor = EventLoopManager.get_executor()

            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(new_loop)
                    result, new_ctx = self._run_coro_with_context(
                        new_loop, func, args, kwargs, caller, ctx
                    )
                    return result, new_ctx
                finally:
                    new_loop.close()

            future = executor.submit(run_in_new_loop)
            result, new_ctx = future.result()
            for var in new_ctx.keys():
                var.set(new_ctx[var])
            return result

        # ─── 不在事件循环线程中 → 可以安全阻塞等待 ───
        result, new_ctx = self._run_coro_with_context(
            loop, func, args, kwargs, caller, ctx
        )
        for var in new_ctx.keys():
            var.set(new_ctx[var])
        return result
```

**死锁防护决策树**：

```
_submit_coro()
│
├─ 不在事件循环线程？
│   └─ YES → 安全阻塞等待（run_coroutine_threadsafe + future.result()）
│
└─ 在事件循环线程中（会死锁！）
    │
    ├─ 有其他可用事件循环（内置循环 ≠ 当前循环）？
    │   └─ YES → 提交到另一个循环，阻塞等待
    │
    └─ 没有其他循环
        └─ 在线程池中创建临时新循环执行，阻塞等待
```

#### 6.2 `_run_coro_with_context` — 上下文传播

```python
    def _run_coro_with_context(
        self,
        loop: asyncio.AbstractEventLoop,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: Dict[str, Any],
        caller: Optional["CallFrame"],
        ctx: contextvars.Context,
    ) -> tuple[Any, contextvars.Context]:
        """在指定事件循环中运行协程并返回上下文

        核心问题：run_coroutine_threadsafe 在另一个线程执行协程，
        协程内部修改的 ContextVar 不会自动传播回调用线程。
        解决方案：协程执行后捕获新上下文，手动同步回调用线程。
        """
        result_holder: list[Any] = []
        ctx_holder: list[contextvars.Context] = []

        async def wrapper():
            result = await self._execute_async(func, args, kwargs, caller)
            # 捕获协程执行后的上下文
            ctx_holder.append(contextvars.copy_context())
            return result

        # 在复制的上下文中运行协程
        future = asyncio.run_coroutine_threadsafe(wrapper(), loop)
        result = future.result()  # 阻塞等待

        new_ctx = ctx_holder[0] if ctx_holder else ctx
        return result, new_ctx
```

**为什么需要上下文传播？**

```
调用线程:                     事件循环线程:
  _submit_coro()               wrapper()
  │                             │
  │  run_coroutine_threadsafe   ├─ _execute_async()
  │  ──────────────────────►    │    └─ func() → 修改了 ContextVar
  │                             │
  │                             ├─ copy_context() → 捕获新上下文
  │                             │
  │  future.result()            │
  │  ◄────────────────────────  │
  │                             │
  ├─ new_ctx[var] = ...  ←─── 手动同步上下文到调用线程
  └─ return result
```

---

### 步骤七：实现统一入口 invoke()

将所有判断逻辑汇聚到一个方法中。

```python
    def invoke(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """执行方法，自动判断类型并统一调用"""
        # 1. 判断方法类型
        kind = self._get_method_kind(func)
        # 2. 获取调用者帧
        caller = self.current_frame
        # 3. 字节码检测 await
        is_awaited = _is_awaited()
        # 4. 判断执行上下文
        in_async = self._is_in_async_context()

        # ─── 同步上下文 ───
        if not in_async:
            if kind == MethodKind.ASYNC:
                # 异步方法 → 提交到事件循环，同步等待
                return self._submit_coro(func, args, kwargs, caller)
            else:
                # 同步方法 → 提交到线程池，同步等待
                return self._submit_sync(func, args, kwargs, caller)

        # ─── 异步上下文 ───
        if kind == MethodKind.ASYNC:
            # 异步方法 → 返回协程
            return self._execute_async(func, args, kwargs, caller)
        else:
            if is_awaited:
                # 同步方法 + await → 包装为协程（线程池执行）
                return self._execute_sync_as_coro(func, args, kwargs, caller)
            else:
                # 同步方法 + 无 await → 直接执行
                return self._execute_sync(func, args, kwargs, caller)
```

**完整决策矩阵**：

```
invoke(func, *args, **kwargs)
│
├─ 1. kind = _get_method_kind(func)
│      └─ SYNC / ASYNC / COROUTINE
│
├─ 2. caller = current_frame
│      └─ 调用栈顶帧（可能为 None）
│
├─ 3. is_awaited = _is_awaited()
│      └─ 字节码检测调用者是否用了 await
│
├─ 4. in_async = _is_in_async_context()
│      └─ 事件循环运行 + 调用者是异步方法
│
├─ 同步上下文 (in_async = False)
│   ├─ ASYNC  → _submit_coro()    [事件循环执行，阻塞等待]
│   └─ SYNC   → _submit_sync()    [线程池执行，阻塞等待]
│
└─ 异步上下文 (in_async = True)
    ├─ ASYNC  → _execute_async()         [返回协程]
    ├─ SYNC + await  → _execute_sync_as_coro() [to_thread 包装，返回协程]
    └─ SYNC + 无await → _execute_sync()        [直接执行，返回结果]
```

---

### 步骤八：实现 RpcMeta + RpcBase（自动方法拦截）

SyncInvoker 本身是底层引擎，用户不应直接调用 `invoker.invoke()`。通过元类自动拦截所有方法调用。

```python
# 全局执行器实例
_invoker: SyncInvoker = SyncInvoker()


def _wrap_method(name: str, func: Callable[..., Any]) -> Callable[..., Any]:
    """包装方法，将调用转发给 SyncInvoker"""

    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        return _invoker.invoke(func, self, *args, **kwargs)

    wrapper.__name__ = name
    wrapper.__qualname__ = func.__qualname__
    wrapper.__wrapped__ = func        # 保留原始方法引用
    return wrapper


class RpcMeta(ABCMeta):
    """元类：拦截类创建，包装所有方法"""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: Dict[str, Any],
    ) -> "RpcMeta":
        new_namespace: Dict[str, Any] = {}

        for attr_name, attr_value in namespace.items():
            # 跳过双下划线方法（__init__, __repr__ 等）
            if attr_name.startswith("__") and attr_name.endswith("__"):
                new_namespace[attr_name] = attr_value
                continue

            # 包装可调用的非类属性
            if callable(attr_value) and not isinstance(attr_value, type):
                wrapped = _wrap_method(attr_name, attr_value)
                # 保留抽象方法标记
                if getattr(attr_value, "__isabstractmethod__", False):
                    wrapped.__isabstractmethod__ = True
                new_namespace[attr_name] = wrapped
            else:
                new_namespace[attr_name] = attr_value

        return super().__new__(mcs, name, bases, new_namespace)


class RpcBase(metaclass=RpcMeta):
    """RPC 基类 — 子类方法自动转发给 SyncInvoker"""

    @classmethod
    def get_invoker(cls) -> SyncInvoker:
        return _invoker
```

**RpcMeta 包装流程**：

```
类定义阶段：
  class MyService(RpcBase):
      def sync_method(self):       ← 原始方法
          return "hello"

      async def async_method(self): ← 原始方法
          return "world"

RpcMeta.__new__ 处理：
  sync_method  → _wrap_method → wrapper(self, *a, **kw): return _invoker.invoke(sync_method, self, *a, **kw)
  async_method → _wrap_method → wrapper(self, *a, **kw): return _invoker.invoke(async_method, self, *a, **kw)

最终类：
  MyService.sync_method  = wrapper（__wrapped__ = 原始 sync_method）
  MyService.async_method = wrapper（__wrapped__ = 原始 async_method）
```

---

### 步骤九：实现 run_callable（轻量级异步执行工具）

```python
async def run_callable(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """在异步上下文中统一执行同步/异步方法

    与 SyncInvoker 的区别：
    - SyncInvoker：完整决策引擎，处理所有上下文组合
    - run_callable：轻量工具，只在异步上下文中使用
    """
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return await asyncio.to_thread(func, *args, **kwargs)
```

**使用场景对比**：

| 工具 | 适用场景 | 复杂度 |
|------|----------|--------|
| `SyncInvoker.invoke()` | 需要自动判断上下文、处理重入 | 高 |
| `run_callable()` | 确定在异步上下文中，只需区分同步/异步 | 低 |

框架中 `ServiceExporter._invoke_method()` 和 `RpcFuture._run_callback()` 使用 `run_callable()`，因为它们确定在异步上下文中运行。

---

## 5. 六种执行场景的完整追踪

### 场景 1：同步上下文 + 同步方法

```python
# 用户代码
service.sync_method()

# 执行追踪
invoke(sync_method, service)
  ├─ kind=SYNC, in_async=False, is_awaited=False
  └─ _submit_sync(sync_method, (service,), {}, None)
      ├─ executor.submit(_execute_sync, ...)
      │   └─ _execute_sync(sync_method, (service,), {}, None)
      │       ├─ _push_frame → CallFrame("sync_method", SYNC, caller=None)
      │       ├─ sync_method(service) → "result"
      │       └─ _pop_frame
      └─ future.result() → "result"
```

### 场景 2：同步上下文 + 异步方法

```python
# 用户代码
service.async_method()

# 执行追踪
invoke(async_method, service)
  ├─ kind=ASYNC, in_async=False, is_awaited=False
  └─ _submit_coro(async_method, (service,), {}, None)
      ├─ loop = EventLoopManager.get_event_loop()
      ├─ in_loop_thread = False
      ├─ _run_coro_with_context(loop, async_method, ...)
      │   ├─ run_coroutine_threadsafe(wrapper(), loop)
      │   │   └─ _execute_async(async_method, ...)
      │   │       ├─ _push_frame → CallFrame("async_method", ASYNC, caller=None)
      │   │       ├─ await async_method(service) → "result"
      │   │       └─ _pop_frame
      │   └─ future.result() → ("result", new_ctx)
      └─ 上下文同步 → "result"
```

### 场景 3：异步上下文 + 异步方法 + await

```python
# 用户代码
await service.async_method()

# 执行追踪
invoke(async_method, service)
  ├─ kind=ASYNC, in_async=True, is_awaited=True
  └─ _execute_async(async_method, (service,), {}, caller)
      ├─ _push_frame → CallFrame("async_method", ASYNC, caller=...)
      ├─ 返回协程对象（由调用者 await）
      └─ await async_method(service) → "result"
      └─ _pop_frame
```

### 场景 4：异步上下文 + 异步方法 + 无 await

```python
# 用户代码
coro = service.async_method()  # 得到协程对象，稍后 await

# 执行追踪 — 与场景 3 完全相同
invoke(async_method, service)
  ├─ kind=ASYNC, in_async=True, is_awaited=False
  └─ _execute_async(async_method, ...) → 返回协程对象
```

### 场景 5：异步上下文 + 同步方法 + await

```python
# 用户代码
result = await service.sync_method()

# 执行追踪
invoke(sync_method, service)
  ├─ kind=SYNC, in_async=True, is_awaited=True
  └─ _execute_sync_as_coro(sync_method, ...)
      ├─ ctx = copy_context()
      ├─ def run_in_thread(): return _execute_sync(sync_method, ...)
      └─ await asyncio.to_thread(ctx.run, run_in_thread)
          └─ [线程池] _execute_sync(sync_method, ...)
              ├─ _push_frame → CallFrame("sync_method", SYNC, caller=...)
              ├─ sync_method(service) → "result"
              └─ _pop_frame
          → "result"
```

### 场景 6：异步上下文 + 同步方法 + 无 await

```python
# 用户代码
result = service.sync_method()  # 直接得到结果

# 执行追踪
invoke(sync_method, service)
  ├─ kind=SYNC, in_async=True, is_awaited=False
  └─ _execute_sync(sync_method, ...)
      ├─ _push_frame → CallFrame("sync_method", SYNC, caller=...)
      ├─ sync_method(service) → "result"
      └─ _pop_frame
```

---

## 6. 重入场景的完整追踪

最复杂的场景：同步→异步→同步 交替调用。

```python
class Service(RpcBase):
    def entry(self) -> str:           # 同步方法
        return self.async_step()

    async def async_step(self) -> str: # 异步方法
        r = await self.sync_step()
        return f"async->{r}"

    def sync_step(self) -> str:        # 同步方法
        return self.async_final()

    async def async_final(self) -> str: # 异步方法
        return "done"

svc = Service()
result = svc.entry()  # → "async->done"
```

```
svc.entry()
│
├─ RpcMeta wrapper → invoke(entry, svc)
│   ├─ kind=SYNC, in_async=False
│   └─ _submit_sync(entry, (svc,), {}, None)  ← 线程池执行
│       │
│       └─ [线程池] _execute_sync(entry, ...)
│           ├─ _push_frame → CallFrame("entry", SYNC, caller=None)
│           │
│           └─ entry() 内部调用 self.async_step()
│               │
│               ├─ RpcMeta wrapper → invoke(async_step, svc)
│               │   ├─ kind=ASYNC, in_async=False（调用者是 SYNC）
│               │   └─ _submit_coro(async_step, ...)
│               │       │
│               │       ├─ 不在事件循环线程
│               │       └─ _run_coro_with_context(loop, async_step, ...)
│               │           │
│               │           └─ [事件循环线程] _execute_async(async_step, ...)
│               │               ├─ _push_frame → CallFrame("async_step", ASYNC, caller=entry_frame)
│               │               │
│               │               └─ await self.sync_step()
│               │                   │
│               │                   ├─ RpcMeta wrapper → invoke(sync_step, svc)
│               │                   │   ├─ kind=SYNC, in_async=True（调用者是 ASYNC）
│               │                   │   ├─ is_awaited=True
│               │                   │   └─ _execute_sync_as_coro(sync_step, ...)
│               │                   │       └─ await asyncio.to_thread(...)
│               │                   │           └─ [线程池] _execute_sync(sync_step, ...)
│               │                   │               ├─ _push_frame → CallFrame("sync_step", SYNC, caller=async_step_frame)
│               │                   │               │
│               │                   │               └─ sync_step() 内部调用 self.async_final()
│               │                   │                   │
│               │                   │                   ├─ RpcMeta wrapper → invoke(async_final, svc)
│               │                   │                   │   ├─ kind=ASYNC, in_async=False（调用者是 SYNC）
│               │                   │                   │   └─ _submit_coro(async_final, ...)
│               │                   │                   │       └─ _run_coro_with_context(loop, ...)
│               │                   │                   │           └─ [事件循环线程] _execute_async(async_final, ...)
│               │                   │                   │               └─ await async_final() → "done"
│               │                   │                   │
│               │                   │               └─ _pop_frame → "async->done"
│               │                   │
│               │                   └─ _pop_frame
│               │
│               └─ _pop_frame
│
└─ _pop_frame → "async->done"
```

**线程切换图**：

```
[主线程/线程池]          [事件循环线程]           [线程池]
    │                        │                      │
    ├─ entry()               │                      │
    │   ├─ async_step() ─────┤                      │
    │   │  submit_coro ──────►                      │
    │   │  阻塞等待           ├─ async_step()        │
    │   │                     │   ├─ sync_step() ────┤
    │   │                     │   │  to_thread ──────►
    │   │                     │   │                  ├─ sync_step()
    │   │                     │   │                  │   ├─ async_final()
    │   │                     │   │                  │   │  submit_coro ──┐
    │   │                     │   │                  │   │               │
    │   │                     │   ◄──────────────────┤   │  run_coro     │
    │   │                     │   ├─ async_final()   │   │               │
    │   │                     │   │  → "done"        │   │               │
    │   ◄─────────────────────┤   │                  │   │               │
    ├─ "async->done"         │                      │   │               │
    │                        │                      │   │               │
```

---

## 7. 框架中的典型使用方式

### 7.1 ObjectProxy（Consumer 端代理）

```python
class ObjectProxy(RpcBase):
    def __getattr__(self, name: str) -> Any:
        # 创建异步方法代理
        async def method_proxy(*args, **kwargs):
            return await self.invoke(name, args, kwargs)

        # 使用 SyncInvoker 包装
        invoker = self.get_invoker()

        def wrapper(*args, **kwargs):
            return invoker.invoke(method_proxy, *args, **kwargs)

        wrapper.__wrapped__ = interface_method
        setattr(self, name, wrapper)
        return wrapper
```

**用户使用**：

```python
# 同步调用（Django 视图）
user = proxy.get_user(123)
# invoke(method_proxy, 123)
#   → kind=ASYNC, in_async=False → _submit_coro → 事件循环执行 → 同步等待

# 异步调用（FastAPI 路由）
user = await proxy.get_user(123)
# invoke(method_proxy, 123)
#   → kind=ASYNC, in_async=True, is_awaited=True → _execute_async → 返回协程
```

### 7.2 RpcFuture（异步结果）

```python
class RpcFuture(RpcBase):
    async def get(self, timeout=None) -> Any:
        """获取结果"""
        future = self._ensure_future()
        return await _await_future_result(future, timeout)
```

**用户使用**：

```python
# 同步获取
result = future.get()
# invoke(get, self)
#   → kind=ASYNC, in_async=False → _submit_coro → 同步等待

# 异步获取
result = await future.get()
# invoke(get, self)
#   → kind=ASYNC, in_async=True, is_awaited=True → _execute_async → 返回协程
```

---

## 8. 类完整实现代码

以下是 SyncInvoker 及其依赖的完整可运行代码：

```python
"""SyncInvoker — 统一同步执行器

核心能力：在同步和异步代码之间提供透明的调用桥接。
"""

import asyncio
import concurrent.futures
import contextvars
import dis
import inspect
import sys
import threading
from abc import ABCMeta
from enum import StrEnum

from pydantic import BaseModel
from typing_extensions import Any, Callable, Dict, List, Optional


# ============================================================
# 辅助类型
# ============================================================

class MethodKind(StrEnum):
    """方法类型枚举"""
    SYNC = "sync"
    ASYNC = "async"
    COROUTINE = "coroutine"


class CallFrame(BaseModel):
    """调用栈帧"""
    method_name: str
    method_kind: MethodKind
    caller: Optional["CallFrame"] = None

    @property
    def caller_kind(self) -> Optional[MethodKind]:
        if self.caller is None:
            return None
        return self.caller.method_kind


# ============================================================
# 模块级状态
# ============================================================

_call_stack: contextvars.ContextVar[List[CallFrame]] = contextvars.ContextVar(
    "_call_stack", default=[],
)

_instruction_cache: Dict[int, List[Any]] = {}


# ============================================================
# 字节码级 await 检测
# ============================================================

def _is_awaited() -> bool:
    try:
        frame = sys._getframe(1)
        frame2 = sys._getframe(2)
        frame2_name = frame2.f_code.co_name

        if frame2_name == "wrapper":
            caller_frame = sys._getframe(3)
        else:
            caller_frame = frame2

        code = caller_frame.f_code
        lasti = caller_frame.f_lasti

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


# ============================================================
# EventLoopManager
# ============================================================

class EventLoopManager:
    """事件循环管理器 — 双模式（外部注入 / 内置创建）"""

    _external_loop: Optional[asyncio.AbstractEventLoop] = None
    _external_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    _internal_loop: Optional[asyncio.AbstractEventLoop] = None
    _internal_thread: Optional[threading.Thread] = None
    _internal_executor: Optional[concurrent.futures.ThreadPoolExecutor] = None
    _started: threading.Event = threading.Event()
    _lock: threading.Lock = threading.Lock()

    @classmethod
    def set_event_loop(cls, loop, executor=None):
        cls._external_loop = loop
        cls._external_executor = executor

    @classmethod
    def clear_event_loop(cls):
        cls._external_loop = None
        cls._external_executor = None

    @classmethod
    def get_event_loop(cls) -> asyncio.AbstractEventLoop:
        if cls._external_loop is not None:
            return cls._external_loop
        return cls._ensure_internal_loop()

    @classmethod
    def get_executor(cls) -> concurrent.futures.ThreadPoolExecutor:
        if cls._external_executor is not None:
            return cls._external_executor
        return cls._ensure_internal_executor()

    @classmethod
    def _ensure_internal_loop(cls) -> asyncio.AbstractEventLoop:
        if cls._internal_loop is not None:
            if cls._internal_loop.is_closed():
                cls._internal_loop = None
                cls._internal_thread = None
            else:
                return cls._internal_loop

        with cls._lock:
            if cls._internal_loop is not None:
                if cls._internal_loop.is_closed():
                    cls._internal_loop = None
                    cls._internal_thread = None
                else:
                    return cls._internal_loop

            try:
                cls._internal_loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

            if cls._internal_loop is None:
                cls._started = threading.Event()
                cls._internal_loop = asyncio.new_event_loop()
                cls._internal_thread = threading.Thread(
                    target=cls._run_internal_loop,
                    daemon=True,
                    name="rpc-bg-loop",
                )
                cls._internal_thread.start()
                cls._started.wait()
            return cls._internal_loop

    @classmethod
    def _run_internal_loop(cls):
        loop = cls._internal_loop
        assert loop is not None
        asyncio.set_event_loop(loop)
        cls._started.set()
        loop.run_forever()

    @classmethod
    def _ensure_internal_executor(cls):
        if cls._internal_executor is not None:
            return cls._internal_executor
        with cls._lock:
            if cls._internal_executor is not None:
                return cls._internal_executor
            cls._internal_executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=10,
                thread_name_prefix="rpc-worker",
            )
            return cls._internal_executor

    @classmethod
    def shutdown(cls):
        if cls._internal_loop is not None:
            cls._internal_loop.call_soon_threadsafe(cls._internal_loop.stop)
            if cls._internal_thread is not None:
                cls._internal_thread.join(timeout=5)
            cls._internal_loop = None
            cls._internal_thread = None
        if cls._internal_executor is not None:
            cls._internal_executor.shutdown(wait=False)
            cls._internal_executor = None


# ============================================================
# SyncInvoker 核心
# ============================================================

class SyncInvoker:
    """统一同步执行器"""

    # ─── 属性 ───

    @property
    def current_frame(self) -> Optional[CallFrame]:
        stack = _call_stack.get()
        if not stack:
            return None
        return stack[-1]

    # ─── 上下文判断 ───

    def _get_method_kind(self, func: Callable[..., Any]) -> str:
        original_func = getattr(func, "__wrapped__", func)
        if asyncio.iscoroutinefunction(original_func) or asyncio.iscoroutinefunction(func):
            return MethodKind.ASYNC
        if inspect.iscoroutine(func):
            return MethodKind.COROUTINE
        return MethodKind.SYNC

    def _is_in_async_context(self) -> bool:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return False
        caller = self.current_frame
        return caller is None or caller.method_kind == MethodKind.ASYNC

    # ─── 调用栈管理 ───

    def _push_frame(self, func, method_kind, caller):
        frame = CallFrame(
            method_name=func.__qualname__,
            method_kind=method_kind,
            caller=caller,
        )
        stack = _call_stack.get().copy()
        stack.append(frame)
        return _call_stack.set(stack)

    def _pop_frame(self, token):
        _call_stack.reset(token)

    # ─── 直接执行（异步上下文） ───

    def _execute_sync(self, func, args, kwargs, caller):
        token = self._push_frame(func, MethodKind.SYNC, caller)
        try:
            return func(*args, **kwargs)
        finally:
            self._pop_frame(token)

    async def _execute_sync_as_coro(self, func, args, kwargs, caller):
        ctx = contextvars.copy_context()
        def run_in_thread():
            return self._execute_sync(func, args, kwargs, caller)
        return await asyncio.to_thread(ctx.run, run_in_thread)

    async def _execute_async(self, func, args, kwargs, caller):
        token = self._push_frame(func, MethodKind.ASYNC, caller)
        try:
            return await func(*args, **kwargs)
        finally:
            self._pop_frame(token)

    # ─── 提交执行（同步上下文） ───

    def _submit_sync(self, func, args, kwargs, caller):
        executor = EventLoopManager.get_executor()
        future = executor.submit(self._execute_sync, func, args, kwargs, caller)
        return future.result()

    def _submit_coro(self, func, args, kwargs, caller):
        loop = EventLoopManager.get_event_loop()
        ctx = contextvars.copy_context()

        try:
            running_loop = asyncio.get_running_loop()
            in_loop_thread = (running_loop is loop)
        except RuntimeError:
            in_loop_thread = False

        if in_loop_thread:
            internal_loop = EventLoopManager._internal_loop
            if internal_loop is not None and internal_loop is not running_loop:
                result, new_ctx = self._run_coro_with_context(
                    internal_loop, func, args, kwargs, caller, ctx
                )
                for var in new_ctx.keys():
                    var.set(new_ctx[var])
                return result

            executor = EventLoopManager.get_executor()
            def run_in_new_loop():
                new_loop = asyncio.new_event_loop()
                try:
                    asyncio.set_event_loop(new_loop)
                    result, new_ctx = self._run_coro_with_context(
                        new_loop, func, args, kwargs, caller, ctx
                    )
                    return result, new_ctx
                finally:
                    new_loop.close()

            future = executor.submit(run_in_new_loop)
            result, new_ctx = future.result()
            for var in new_ctx.keys():
                var.set(new_ctx[var])
            return result

        result, new_ctx = self._run_coro_with_context(
            loop, func, args, kwargs, caller, ctx
        )
        for var in new_ctx.keys():
            var.set(new_ctx[var])
        return result

    def _run_coro_with_context(self, loop, func, args, kwargs, caller, ctx):
        result_holder = []
        ctx_holder = []

        async def wrapper():
            result = await self._execute_async(func, args, kwargs, caller)
            ctx_holder.append(contextvars.copy_context())
            return result

        future = asyncio.run_coroutine_threadsafe(wrapper(), loop)
        result = future.result()
        new_ctx = ctx_holder[0] if ctx_holder else ctx
        return result, new_ctx

    # ─── 统一入口 ───

    def invoke(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        kind = self._get_method_kind(func)
        caller = self.current_frame
        is_awaited = _is_awaited()
        in_async = self._is_in_async_context()

        if not in_async:
            if kind == MethodKind.ASYNC:
                return self._submit_coro(func, args, kwargs, caller)
            else:
                return self._submit_sync(func, args, kwargs, caller)

        if kind == MethodKind.ASYNC:
            return self._execute_async(func, args, kwargs, caller)
        else:
            if is_awaited:
                return self._execute_sync_as_coro(func, args, kwargs, caller)
            else:
                return self._execute_sync(func, args, kwargs, caller)


# ============================================================
# 全局实例 + 元类 + 基类
# ============================================================

_invoker: SyncInvoker = SyncInvoker()


def _wrap_method(name: str, func: Callable[..., Any]) -> Callable[..., Any]:
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
        return _invoker.invoke(func, self, *args, **kwargs)
    wrapper.__name__ = name
    wrapper.__qualname__ = func.__qualname__
    wrapper.__wrapped__ = func
    return wrapper


class RpcMeta(ABCMeta):
    def __new__(mcs, name, bases, namespace):
        new_namespace = {}
        for attr_name, attr_value in namespace.items():
            if attr_name.startswith("__") and attr_name.endswith("__"):
                new_namespace[attr_name] = attr_value
                continue
            if callable(attr_value) and not isinstance(attr_value, type):
                wrapped = _wrap_method(attr_name, attr_value)
                if getattr(attr_value, "__isabstractmethod__", False):
                    wrapped.__isabstractmethod__ = True
                new_namespace[attr_name] = wrapped
            else:
                new_namespace[attr_name] = attr_value
        return super().__new__(mcs, name, bases, new_namespace)


class RpcBase(metaclass=RpcMeta):
    @classmethod
    def get_invoker(cls) -> SyncInvoker:
        return _invoker


# ============================================================
# 轻量级异步执行工具
# ============================================================

async def run_callable(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    if asyncio.iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return await asyncio.to_thread(func, *args, **kwargs)
```

---

## 9. 方法索引与职责速查

| 方法 | 调用时机 | 执行线程 | 返回类型 | 核心机制 |
|------|----------|----------|----------|----------|
| `_execute_sync` | 异步上下文+SYNC+无await | 当前线程 | 结果 | 直接调用 |
| `_execute_sync_as_coro` | 异步上下文+SYNC+await | 线程池 | 协程 | `asyncio.to_thread` |
| `_execute_async` | 异步上下文+ASYNC | 当前线程(事件循环) | 协程 | `await` |
| `_submit_sync` | 同步上下文+SYNC | 线程池 | 结果 | `executor.submit` + `future.result()` |
| `_submit_coro` | 同步上下文+ASYNC | 事件循环线程 | 结果 | `run_coroutine_threadsafe` + 死锁防护 |
| `_run_coro_with_context` | `_submit_coro`内部 | 事件循环线程 | (结果, 上下文) | 上下文捕获与传播 |
| `_get_method_kind` | `invoke`入口 | - | MethodKind | `__wrapped__`解包 + `iscoroutinefunction` |
| `_is_in_async_context` | `invoke`入口 | - | bool | `get_running_loop` + 调用栈帧 |
| `_is_awaited` | `invoke`入口 | - | bool | 字节码 `GET_AWAITABLE` 检测 |
| `_push_frame` | 执行前 | - | Token | ContextVar set |
| `_pop_frame` | 执行后 | - | None | ContextVar reset |
