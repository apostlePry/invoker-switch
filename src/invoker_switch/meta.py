"""InvokerMeta 元类与 InvokerBase 基类 — 自动拦截方法调用并转发给 SyncInvoker"""

from abc import ABCMeta

from typing_extensions import Any, Callable, Dict, Tuple, cast

from .invoker import SyncInvoker

# ─── 全局执行器实例 ───

_invoker: SyncInvoker = SyncInvoker()


class InvokerMeta(ABCMeta):
    """元类：拦截类创建，包装所有方法

    _wrap_method 作为类方法而非模块级函数，好处：
    1. 职责内聚 —— 包装逻辑和使用它的元类在同一个类中
    2. 可覆写 —— InvokerMeta 子类可以定制包装策略
    3. invoker 来源可扩展 —— 通过 _get_invoker() 获取，而非硬编码全局变量
    """

    @classmethod
    def _get_invoker(cls) -> SyncInvoker:
        """获取 invoker 实例，子类可覆写此方法来定制 invoker 来源"""
        return _invoker

    @classmethod
    def _wrap_method(cls, name: str, func: Callable[..., Any]) -> Callable[..., Any]:
        """包装方法，将调用转发给 SyncInvoker

        Args:
            name: 方法名
            func: 原始方法

        Returns:
            包装后的方法，调用时自动转发给 SyncInvoker.invoke()
        """
        invoker = cls._get_invoker()

        # 获取原始方法的返回类型
        return_type = func.__annotations__.get('return', Any)

        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = invoker.invoke(func, self, *args, **kwargs)
            # 使用 cast 告诉 mypy 返回类型，避免类型检查报错
            return cast(return_type, result)

        # 保留原始方法的类型注解
        wrapper.__annotations__ = func.__annotations__
        wrapper.__name__ = name
        wrapper.__qualname__ = func.__qualname__
        wrapper.__wrapped__ = func  # 保留原始方法引用
        return wrapper

    def __new__(
        mcs,
        name: str,
        bases: Tuple[type, ...],
        namespace: Dict[str, Any],
    ) -> "InvokerMeta":
        new_namespace: Dict[str, Any] = {}

        for attr_name, attr_value in namespace.items():
            # 跳过双下划线方法（__init__, __repr__ 等）
            if attr_name.startswith("__") and attr_name.endswith("__"):
                new_namespace[attr_name] = attr_value
                continue

            # 包装可调用的非类属性
            if callable(attr_value) and not isinstance(attr_value, type):
                wrapped = mcs._wrap_method(attr_name, attr_value)
                # 保留抽象方法标记
                if getattr(attr_value, "__isabstractmethod__", False):
                    wrapped.__isabstractmethod__ = True
                new_namespace[attr_name] = wrapped
            else:
                new_namespace[attr_name] = attr_value

        return super().__new__(mcs, name, bases, new_namespace)


class InvokerBase(metaclass=InvokerMeta):
    """Invoker 基类 — 子类方法自动转发给 SyncInvoker"""

    @classmethod
    def get_invoker(cls) -> SyncInvoker:
        """获取全局 invoker 实例"""
        return _invoker
