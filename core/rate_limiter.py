"""
Асинхронный алгоритм «ведро токенов» (token-bucket) для ограничения
частоты HTTP-запросов к бирже BingX.

Использование:
    limiter = RateLimiter(rps=2)      # ≤2 запроса в секунду
    await limiter.acquire()           # ждёт, пока появится токен

✔ Поддерживает конкурентные вызовы (asyncio.Lock).
✔ Не даёт перерасходовать лимит даже при множестве Tasks.
✔ Минимальная задержка сна — 50 мс, чтобы не грузить CPU.
"""

from __future__ import annotations

import asyncio
import time


class RateLimiter:
    """
    rps — «requests per second» = вместимость ведра.
    Токены «наливаются» со скоростью capacity / сек.
    """

    def __init__(self, rps: int) -> None:
        if rps <= 0:
            raise ValueError("rps must be > 0")
        self._capacity: float = float(rps)   # макс. кол-во токенов
        self._tokens: float = float(rps)     # текущее кол-во
        self._last_refill: float = time.monotonic()
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    async def acquire(self) -> None:
        """
        Блокируется, пока не появится хотя бы один токен.
        Цикл под защитой Lock — конкурентные задачи не «выбьют» лишние
        токены друг у друга.
        """
        async with self._lock:
            while True:
                now: float = time.monotonic()
                delta: float = now - self._last_refill
                self._last_refill = now

                # «наливаем» новые токены, но не превышаем capacity
                self._tokens = min(
                    self._capacity,
                    self._tokens + delta * self._capacity,
                )

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                # не спим совсем 0 — жжёт CPU; 50 мс достаточно мелко
                await asyncio.sleep(0.05)
