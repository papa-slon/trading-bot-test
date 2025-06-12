"""
core.price_feed
───────────────────────────────────────────────────────────────────────────────
Singleton-шина цен для всех ботов.

• подписывает символы на публичный BingX-WebSocket (≤200 символов/соед.);
• гарантирует, что каждый символ идёт только в одном WS-канале;
• рассылает цены во все зарегистрированные callbacks;
• не использует deepcopy() — безопасно для asyncio.Task.

Зависит от core.websockets_updates.BingxWebsocket (один канал = один WS).

Интерфейс
──────────
    feed = PriceFeed.instance(client)     # client — BingXClient
    feed.subscribe(["BTC-USDT"], callback)
    await feed.aclose()                   # при shutdown

callback: Callable[[symbol:str, price:float, ts:float], None]
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Callable, Dict, Iterable, List, Set

from core.websockets_updates import BingxWebsocket

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
#  PriceFeed singleton
# ──────────────────────────────────────────────────────────────────────────────
class PriceFeed:
    _instance: "PriceFeed | None" = None

    # ---------------------------------------------------------------- ctor
    def __init__(self) -> None:
        self._ws_channels: Dict[int, BingxWebsocket] = {}       # id(ws)→obj
        self._callbacks: Dict[str, Set[Callable]] = {}          # symbol→set(cb)

    # ---------------------------------------------------------------- instance
    @classmethod
    def instance(cls) -> "PriceFeed":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    # ---------------------------------------------------------------- subscribe
    def subscribe(
        self,
        symbols: Iterable[str],
        callback: Callable[[str, float, float], None],
    ) -> None:
        """
        Регистрирует callback и, при нехватке подписки, создаёт/расширяет WS.
        """
        new_syms: List[str] = []
        for s in symbols:
            su = s.upper()
            if su not in self._callbacks:
                self._callbacks[su] = set()
                new_syms.append(su)
            self._callbacks[su].add(callback)

        if new_syms:
            asyncio.create_task(self._ensure_ws(new_syms))

    # ---------------------------------------------------------------- _ensure_ws
    async def _ensure_ws(self, symbols: List[str]) -> None:
        """
        Либо дописывает символы в существующий WS (если ещё <200),
        либо создаёт новый BingxWebsocket.
        """
        for ws in self._ws_channels.values():
            if len(ws.symbols) + len(symbols) <= 200:
                ws.symbols.extend(symbols)          # type: ignore[attr-defined]
                return

        ws = BingxWebsocket(symbols, self._dispatch)
        self._ws_channels[id(ws)] = ws
        asyncio.create_task(ws.run_forever(), name="PriceFeedWS")

    # ---------------------------------------------------------------- _dispatch
    def _dispatch(self, sym: str, price: float, ts: float) -> None:
        """
        Рассылает цену всем подписчикам; shallow-copy списка callback'ов,
        чтобы не держать lock и не копировать asyncio.Task.
        """
        for cb in list(self._callbacks.get(sym, ())):
            try:
                cb(sym, price, ts)
            except Exception:
                logger.exception("price callback error [%s]", sym)

    # ---------------------------------------------------------------- aclose
    async def aclose(self) -> None:
        """
        Завершает все WebSocket-каналы.
        """
        await asyncio.gather(*(ws.aclose() for ws in self._ws_channels.values()))
        self._ws_channels.clear()
        self._callbacks.clear()
