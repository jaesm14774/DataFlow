import gc
import inspect
import functools


def release_memory():
    gc.collect()


def task_wrapper(func):
    sig = inspect.signature(func)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        try:
            return func(*args, **filtered_kwargs)
        finally:
            release_memory()
    return wrapper
