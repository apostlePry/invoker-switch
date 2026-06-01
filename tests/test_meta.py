"""InvokerMeta 元类测试"""

import asyncio
from abc import abstractmethod

import pytest

from invoker_switch import InvokerBase, InvokerMeta, SyncInvoker, WRAPPER_FUNC_NAME


class TestInvokerMetaWrapping:
    """测试 InvokerMeta 的方法包装行为"""

    def test_sync_method_is_wrapped(self):
        """同步方法应被包装，但通过 invoke 仍可正常调用"""

        class Svc(InvokerBase):
            def hello(self):
                return "hi"

        svc = Svc()
        assert svc.hello() == "hi"

    def test_async_method_is_wrapped(self):
        """异步方法应被包装，但通过 invoke 仍可正常调用"""

        class Svc(InvokerBase):
            async def hello(self):
                return "hi"

        svc = Svc()
        assert svc.hello() == "hi"

    def test_dunder_not_wrapped(self):
        """双下划线方法不应被包装"""

        class Svc(InvokerBase):
            def __init__(self, name):
                self.name = name

            def __repr__(self):
                return f"Svc({self.name})"

        svc = Svc("test")
        assert svc.name == "test"
        assert repr(svc) == "Svc(test)"
        # __init__ 和 __repr__ 不应有 __wrapped__
        assert not hasattr(Svc.__init__, "__wrapped__")
        assert not hasattr(Svc.__repr__, "__wrapped__")

    def test_class_attribute_not_wrapped(self):
        """类属性（非 callable）不应被包装"""

        class Svc(InvokerBase):
            version = "1.0"

        assert Svc.version == "1.0"

    def test_abstract_method_preserved(self):
        """抽象方法标记应被保留"""

        class BaseSvc(InvokerBase):
            @abstractmethod
            def must_implement(self):
                ...

        # __isabstractmethod__ 应为 True
        assert getattr(BaseSvc.must_implement, "__isabstractmethod__", False) is True

    def test_wrapper_preserves_name(self):
        """wrapper 的 __name__ 和 __qualname__ 应与原始方法一致"""

        class Svc(InvokerBase):
            def my_method(self):
                return 42

        assert Svc.my_method.__name__ == "my_method"

    def test_wrapper_preserves_wrapped(self):
        """wrapper 的 __wrapped__ 应指向原始方法"""

        class Svc(InvokerBase):
            def my_method(self):
                return 42

        assert hasattr(Svc.my_method, "__wrapped__")
        # __wrapped__ 是原始方法
        assert Svc.my_method.__wrapped__ is not None


class TestInvokerMetaCustomInvoker:
    """测试 InvokerMeta._get_invoker 和 _wrap_method 的可覆写性"""

    def test_get_invoker_returns_global(self):
        """默认 _get_invoker 返回全局 _invoker"""
        invoker = InvokerMeta._get_invoker()
        assert isinstance(invoker, SyncInvoker)

    def test_wrap_method_creates_wrapper(self):
        """_wrap_method 应创建正确的 wrapper"""

        def original(self, x):
            return x * 2

        wrapper = InvokerMeta._wrap_method("double", original)
        assert wrapper.__name__ == "double"
        assert wrapper.__wrapped__ is original

    def test_wrapper_func_name_constant(self):
        """_WRAPPER_FUNC_NAME 应为 'wrapper'"""
        assert InvokerMeta._WRAPPER_FUNC_NAME == "wrapper"
        assert WRAPPER_FUNC_NAME == "wrapper"


class TestInvokerMetaInheritance:
    """测试 InvokerMeta 的继承行为"""

    def test_subclass_inherits_wrapping(self):
        """子类方法也应被自动包装"""

        class Parent(InvokerBase):
            def parent_method(self):
                return "parent"

        class Child(Parent):
            def child_method(self):
                return "child"

        child = Child()
        assert child.parent_method() == "parent"
        assert child.child_method() == "child"

    def test_subclass_override(self):
        """子类覆写的方法也应被包装"""

        class Parent(InvokerBase):
            def greet(self):
                return "parent"

        class Child(Parent):
            def greet(self):
                return "child"

        child = Child()
        assert child.greet() == "child"
