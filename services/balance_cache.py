"""
services/balance_cache.py
Баланс кешируется 30 с на пару (api_id, account-type).
"""

from __future__ import annotations

import asyncio
import time
from typing import Final, Tuple

_TTL_BALANCE: Final[float] = 30.0


class BalanceCache:
    _balances: dict[Tuple[str, str], float] = {}
    _ts: dict[Tuple[str, str], float] = {}
    _lock = asyncio.Lock()

    @classmethod
    async def set(cls, api_id: str, account_type: str, balance: float) -> None:
        key = (api_id, account_type)
        async with cls._lock:
            cls._balances[key] = balance
            cls._ts[key] = time.time()

    @classmethod
    async def get(cls, api_id: str, account_type: str) -> float | None:
        key = (api_id, account_type)
        async with cls._lock:
            if key not in cls._balances:
                return None
            if time.time() - cls._ts[key] > _TTL_BALANCE:
                return None
            return cls._balances[key]
