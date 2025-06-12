"""
utils/throttle.py
Универсальные декораторы: (а) TTL-кеш, (б) ограничитель RPS, (в) retry c back-off.
Без сторонних зависимостей.
"""

from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

_T = TypeVar("_T")

# ──────────────────────────── RPS / Semaphore ────────────────────────────
# общий пул «разрешений» на запросы. Один пул на один API-ключ
_rate_limiters: dict[str, asyncio.Semaphore] = {}


def rate_limited(max_rps: int, key: str) -> Callable[[Callable[..., Awaitable[_T]]], Callable[..., Awaitable[_T]]]:
    """
    Оборачивает корутин-запросы к внешнему REST и гарантирует
    не более `max_rps` за последние 1 сек. на «ключ».
    """
    sem = _rate_limiters.setdefault(key, asyncio.Semaphore(max_rps))

    def decorator(func: Callable[..., Awaitable[_T]]) -> Callable[..., Awaitable[_T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> _T:
            async with sem:                 # «взяли слот»
                return await func(*args, **kwargs)

        return wrapper

    return decorator


# ───────────────────────────── TTL-cache ────────────────────────────────
def ttl_cache(ttl: float, name: str | None = None) -> Callable[[Callable[..., Awaitable[_T]]], Callable[..., Awaitable[_T]]]:
    """
    Декоратор-кеш с TTL (сек). Ключ — позиционные аргументы + имя (по-умолч.)
    """
    _store: dict[tuple[Any, ...], tuple[_T, float]] = {}
    _lock = asyncio.Lock()

    def decorator(func: Callable[..., Awaitable[_T]]) -> Callable[..., Awaitable[_T]]:
        _prefix = (name or func.__qualname__,)

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> _T:
            key = _prefix + args + tuple(sorted(kwargs.items()))
            now = time.time()

            async with _lock:
                if key in _store and now - _store[key][1] < ttl:
                    return _store[key][0]

            result = await func(*args, **kwargs)

            async with _lock:
                _store[key] = (result, now)

            return result

        return wrapper

    return decorator


# ─────────────────────────── back-off & retry ───────────────────────────
def with_retry(retries: int = 3, base_delay: float = 0.5, exc: type[Exception] = Exception) -> Callable[[Callable[..., Awaitable[_T]]], Callable[..., Awaitable[_T]]]:
    """
    Повторяет вызов с экспоненциальным back-off`ом.
    """
    def decorator(func: Callable[..., Awaitable[_T]]) -> Callable[..., Awaitable[_T]]:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> _T:
            for attempt in range(1, retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exc as e:
                    if attempt == retries:
                        raise
                    await asyncio.sleep(base_delay * 2 ** (attempt - 1))  # back-off

        return wrapper

    return decorator
