import gc
import inspect

def release_memory():
    """强制执行垃圾回收，释放内存"""
    gc.collect()
    
def task_wrapper(func):
    """装饰器：在任务执行完成后自动释放内存"""
    def wrapper(*args, **kwargs):
        # Get the signature of the wrapped function
        sig = inspect.signature(func)
        # Filter kwargs to only include parameters the function accepts
        filtered_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        try:
            return func(*args, **filtered_kwargs)
        finally:
            release_memory()
    return wrapper
