import inspect
from collections.abc import Awaitable
from typing import (
    TypeVar,
    Union,
)

T = TypeVar("T")

AwaitableOrValue = Union[Awaitable[T], T]


async def await_maybe(value: AwaitableOrValue[T]) -> T:
    if inspect.isawaitable(value):
        return await value

    return value
