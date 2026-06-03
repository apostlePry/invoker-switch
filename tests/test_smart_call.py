import asyncio
import functools
import threading

from typing_extensions import Callable, TypeVar

from invoker_switch.invoker import SyncInvoker
from invoker_switch.detection import mark_wrapper


_invoker = SyncInvoker()
T = TypeVar("T")


def smart_call(func: Callable):
    @functools.wraps(func)
    def _wrapper_call(*args, **kwargs) -> T:
        return _invoker.invoke(func, *args, **kwargs)
    mark_wrapper(_wrapper_call)
    return _wrapper_call


@smart_call
async def demo():
    print("Execute demo function!")
    await asyncio.sleep(1)
    return "Demo completed"


@smart_call
def hello():
    print("Hello in thread: {}".format(threading.current_thread().name))
    result = demo()
    print(result)
    return "Hello completed"


@smart_call
async def world():
    print("World in thread: {}".format(threading.current_thread().name))
    result = await hello()
    print("Function hello execute completed, get result is: {}".format(result))
    await asyncio.sleep(1)
    return "completed"


@smart_call
def func_demo():
    print("Func demo in thread: {}".format(threading.current_thread().name))
    result = world()
    print("In func demo, world is execute completed! get result: {}".format(result))
    result = hello()
    print("In func demo, hello is execute completed! get result: {}".format(result))
    return result


def test_context_run_with_sync():
    result = world()
    assert result
    print(result)
