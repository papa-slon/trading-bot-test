"""
market_api/binance_client.py
Публичный клиент Binance для ценового потока 1-минутных свечей.
REST • WS без авторизации.
"""

from __future__ import annotations
import time
import aiohttp
import asyncio
import json
from typing import Any, Callable, Dict, List, Sequence

_BASE_REST = "https://fapi.binance.com"
_BASE_WS   = "wss://fstream.binance.com:9443/stream"


class BinanceClient:
    def __init__(self) -> None:
        self._session: aiohttp.ClientSession | None = None

    # ────────────────────────── REST - klines ──────────────────────────
    async def get_klines(
        self,
        symbol: str,
        *,
        interval: str = "1m",
        limit: int = 200,
    ) -> Sequence[Any]:
        MAXIMUM = 1000
        if self._session is None:
            self._session = aiohttp.ClientSession()
        i = 1
        k = 0
        if limit > MAXIMUM:
            i = limit // MAXIMUM
            k = limit % MAXIMUM
            limit = MAXIMUM
        url = f"{_BASE_REST}/fapi/v1/klines"
        ret = []
        end = int(time.time()*1000)
        start = end - limit*60_000
        #print(i)
        for l in range(i):
            params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": start, "endTime": end}
            #print(params)
            async with self._session.get(url, params=params, timeout=10) as resp:
                res = await resp.json()
               # print(res)
                resp.raise_for_status()
            start -= limit * 60_000
            end -= limit * 60_000
            ret +=res

            #await asyncio.sleep(1)
        if k > 0:
            params = {"symbol": symbol, "interval": interval, "limit": limit, "startTime": start}
            async with self._session.get(url, params=params, timeout=10) as resp:
                resp.raise_for_status()
                res = await resp.json()

            ret += res
        #print(ret)
        return ret

    # ──────────────────────── WS - kline_1m ────────────────────────────
    async def start_kline_ws(                                  # ← нужная функция
        self,
        symbols: List[str],
        on_kline: Callable[[str, Dict[str, Any]], None],
    ) -> None:
        """
        Подписываемся на закрытые kline_1m для списка symbols.
        • symbols = ["BNBUSDT", "BTCUSDT"]
        • on_kline(symbol:str, k:dict) вызывается КАЖДУЮ минуту,
          когда поле k["x"] == True (свеча закрыта).
        """
        if self._session is None:
            self._session = aiohttp.ClientSession()

        streams = "/".join(f"{s.lower()}@kline_1m" for s in symbols)
        url = f"{_BASE_WS}?streams={streams}"

        while True:                       # бесконечный авто-reconnect
            try:
                async with self._session.ws_connect(url, heartbeat=20) as ws:
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)

                            # Binance может прислать ошибку вместо свечи
                            if "code" in data:                   # {'code':-1121,…}
                                print(f"[Binance WS] server error {data}")
                                continue

                            k = data["data"]["k"]
                            if k["x"]:                           # свеча закрыта
                                on_kline(k["s"], k)              # callback
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
            except Exception as e:
                print(f"[Binance WS] error {e}, reconnect in 5 s")
                await asyncio.sleep(5)                           # пауза перед retry

    # ───────────────────────── utils ─────────────────────────
    async def close(self) -> None:
        if self._session:
            await self._session.close()
