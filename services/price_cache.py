"""
services/price_cache.py

Потокобезопасный кэш цен v2 — с адаптивным TTL, пакетным обновлением,
фоновой очисткой и без «фантомных» 0-цен.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Final, Dict, List

logger = logging.getLogger(__name__)


def _monotonic() -> float:
    """Обособленная обёртка для unit-тестов."""
    return time.monotonic()


class PriceCache:
    """
    • get(symbol[, max_age])  →  float | None
    • update(symbol, price)
    • bulk_update({symbol: price, …})
    Интерфейс прежний, но реализация избавлена от defaultdict.
    """

    _prices: Dict[str, float] = {}
    _timestamp: Dict[str, float] = {}
    _symbol_ttl: Dict[str, float] = {}

    _DEFAULT_TTL: Final[float] = 60.0     # базово — 1 минута
    _MAX_TTL:     Final[float] = 600.0    # back-off до 10 мин

    _thread_lock = threading.RLock()
    _async_lock  = asyncio.Lock()

    # ───────────────────────── public API ───────────────────────── #

    @classmethod
    async def update(cls, symbol: str, price: float) -> None:
        now = _monotonic()
        async with cls._async_lock:
            with cls._thread_lock:
                cls._prices[symbol]    = price
                cls._timestamp[symbol] = now
                cls._symbol_ttl.pop(symbol, None)

    @classmethod
    async def bulk_update(cls, data: Dict[str, float]) -> None:
        now = _monotonic()
        async with cls._async_lock:
            with cls._thread_lock:
                cls._prices.update(data)
                for s in data:
                    cls._timestamp[s] = now
                    cls._symbol_ttl.pop(s, None)

    @classmethod
    async def get(cls, symbol: str, max_age: float | None = None) -> float | None:
        ttl = max_age if max_age is not None else cls._symbol_ttl.get(symbol, cls._DEFAULT_TTL)

        async with cls._async_lock:
            ts = cls._timestamp.get(symbol)
            if ts is None or _monotonic() - ts > ttl:
                # увеличиваем TTL вдвое (до 10 мин) — экспоненциальный back-off
                cls._symbol_ttl[symbol] = min(
                    cls._symbol_ttl.get(symbol, cls._DEFAULT_TTL) * 2 or cls._DEFAULT_TTL,
                    cls._MAX_TTL,
                )
                return None
            return cls._prices.get(symbol)

    # ───────────────────── housekeeping (GC) ───────────────────── #

    @classmethod
    async def _gc(cls, every: int = 300) -> None:
        while True:
            await asyncio.sleep(every)
            now = _monotonic()
            stale: List[str] = [s for s, ts in cls._timestamp.items() if now - ts > 3600]
            if stale:
                async with cls._async_lock:
                    with cls._thread_lock:
                        for s in stale:
                            cls._prices.pop(s, None)
                            cls._timestamp.pop(s, None)
                            cls._symbol_ttl.pop(s, None)
                        logger.debug("PriceCache GC: %d stale symbols removed", len(stale))

    @classmethod
    def start_gc_task(cls, loop: asyncio.AbstractEventLoop | None = None) -> None:
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
        if not getattr(cls, "_gc_started", False):
            loop.create_task(cls._gc())
            cls._gc_started = True


# автоматический запуск GC (если loop уже существует)
try:
    PriceCache.start_gc_task()
except RuntimeError:
    # no running event loop yet
    pass
