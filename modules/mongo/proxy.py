import functools
import types
from typing import TypeVar

import pymongo.errors


T = TypeVar('T')


class MongoProxyObject:
    def __init__(self, klass, *args, **kwargs):
        self._instance = klass(*args, **kwargs)
        self._obj = None

    def __getattr__(self, item):
        func = getattr(self._instance, item)

        def wrapper(*args, **kwargs):
            instance = func(*args, **kwargs)

            if isinstance(instance, types.AsyncGeneratorType):
                async def inner():
                    try:
                        async for v in instance:
                            yield v
                    except (pymongo.errors.NotMasterError,
                            pymongo.errors.ServerSelectionTimeoutError):
                        raise
            else:
                async def inner():
                    try:
                        return await instance
                    except (pymongo.errors.NotMasterError,
                            pymongo.errors.ServerSelectionTimeoutError):
                        raise
            return inner()
        return wrapper


def make_proxy(klass: T) -> T:
    return functools.partial(MongoProxyObject, klass)
