# invoker-switch

**SyncInvoker — 统一同步/异步执行器**，透明桥接同步与异步调用，让用户完全不需要关心同步/异步边界。

## 安装

```bash
uv add invoker-switch
```

## 快速开始

```python
from invoker_switch import RpcBase


class UserService(RpcBase):
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
class Service(RpcBase):
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

## API

- `RpcBase` — RPC 基类，子类方法自动转发给 SyncInvoker
- `RpcMeta` — 元类，拦截类创建并包装所有方法
- `SyncInvoker` — 核心执行器，自动判断执行策略
- `EventLoopManager` — 事件循环管理器（外部注入/内置创建）
- `MethodKind` — 方法类型枚举（SYNC/ASYNC/COROUTINE）
- `CallFrame` — 调用栈帧
- `run_callable()` — 轻量级异步执行工具

## 开发

```bash
# 安装开发依赖
uv sync --group dev

# 运行测试
uv run pytest tests/ -v
```

## License

MIT