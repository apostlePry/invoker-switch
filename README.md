# invoker-switch

**统一同步/异步执行器**，透明桥接同步与异步调用，让用户完全不需要关心同步/异步边界。

## 安装

```bash
uv add invoker-switch
```

## 快速开始

### 通过 InvokerBase 声明式使用

```python
from invoker_switch import InvokerBase


class UserService(InvokerBase):
    def get_name(self) -> str:
        return "Alice"

    async def fetch_avatar(self) -> str:
        return "https://avatar.example.com/alice.png"


svc = UserService()

# 同步上下文 — 无论方法定义是 sync 还是 async，统一调用
name = svc.get_name()        # → "Alice"
avatar = svc.fetch_avatar()  # → "https://avatar.example.com/alice.png"

# 异步上下文 — 支持 await
async def main():
    name = svc.get_name()              # 直接得到结果
    avatar = await svc.fetch_avatar()  # 返回协程
```

### 通过 run_callable 函数式使用

```python
from invoker_switch import run_callable

def sync_work():
    return "sync_result"

async def async_work():
    return "async_result"

# 同步上下文中调用 — 统一返回结果
run_callable(sync_work)    # → "sync_result"
run_callable(async_work)   # → "async_result"

# 异步上下文中调用 — 统一返回结果
async def main():
    result = await run_callable(sync_work)   # → "sync_result"
    result = await run_callable(async_work)  # → "async_result"
```

## 核心概念

### 六种执行场景

| 上下文 | 方法类型 | await | 执行策略 |
|--------|---------|-------|---------|
| 同步 | SYNC | - | 线程池执行，阻塞等待 |
| 同步 | ASYNC | - | 事件循环执行，阻塞等待 |
| 异步 | ASYNC | ✓ | 返回协程 |
| 异步 | ASYNC | ✗ | 返回协程 |
| 异步 | SYNC | ✓ | `to_thread` 包装，返回协程 |
| 异步 | SYNC | ✗ | 当前线程直接执行 |

### 重入安全

支持 同步→异步→同步→异步 交替调用，自动处理死锁防护。

```python
class Service(InvokerBase):
    def entry(self) -> str:
        return self.async_step()  # 同步调用异步

    async def async_step(self) -> str:
        r = await self.sync_step()  # 异步调用同步
        return f"async->{r}"

    def sync_step(self) -> str:
        return self.async_final()  # 同步调用异步

    async def async_final(self) -> str:
        return "done"

svc = Service()
result = svc.entry()  # → "async->done"
```

### 两种使用方式对比

| 特性 | `InvokerBase` 声明式 | `run_callable` 函数式 |
|------|----------------------|----------------------|
| 使用方式 | 继承基类，方法自动桥接 | 逐个函数调用 |
| 同步/异步上下文 | 自动适配 | 自动适配 |
| await 感知 | ✓ 字节码级检测 | ✗ 不做 await 检测 |
| 调用栈追踪 | ✓ 完整 CallFrame 链 | ✗ 无调用栈 |
| 重入死锁防护 | ✓ 完整防护 | ✗ 无防护 |
| 适用场景 | 服务类、长期维护的代码 | 简单调用、一次性桥接 |

## API

### 核心

- **`InvokerBase`** — 基类，子类方法自动转发给 SyncInvoker
- **`InvokerMeta`** — 元类，拦截类创建并包装所有方法
- **`SyncInvoker`** — 核心执行器，自动判断执行策略

### 工具

- **`run_callable(func, *args, **kwargs)`** — 轻量级统一执行工具，同步/异步上下文均可使用
  - 同步上下文：调用同步函数直接执行，调用异步函数提交到事件循环阻塞等待
  - 异步上下文：调用同步函数通过 `to_thread` 执行，调用异步函数直接 `await`

### 基础设施

- **`EventLoopManager`** — 事件循环管理器（外部注入 / 内置创建）

### 类型

- **`MethodKind`** — 方法类型枚举（`SYNC` / `ASYNC` / `COROUTINE`）
- **`CallFrame`** — 调用栈帧

## 项目结构

```
src/invoker_switch/
├── __init__.py      # 公共 API 导出
├── types.py         # MethodKind, CallFrame, _call_stack
├── detection.py     # 字节码级 await 检测 (is_awaited)
├── loop.py          # EventLoopManager 事件循环管理
├── invoker.py       # SyncInvoker 核心执行器
├── meta.py          # InvokerMeta 元类, InvokerBase 基类
└── utils.py         # run_callable 统一执行工具
```

## 开发

```bash
# 安装开发依赖
uv sync --group dev

# 运行测试
uv run pytest tests/ -v
```

## License

MIT