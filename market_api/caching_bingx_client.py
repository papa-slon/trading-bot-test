# market_api/caching_bingx_client.py

import time
import asyncio
import configparser
from aiohttp import ClientSession, ClientError
from aiocache import cached, Cache

from market_api.core import MarketClient, TickerKline
from sqlalchemy_enums import MarketEnum
from core.alert_server import logger

# ─────── Настройки из config.ini ───────
_cfg = configparser.ConfigParser()
_cfg.read("config.ini")

KLINES_TTL = _cfg.getint("API_CACHE", "klines_ttl", fallback=8)
PRICE_TTL  = _cfg.getint("API_CACHE", "price_ttl",  fallback=1)
RETRIES    = _cfg.getint("API_RETRY",  "retries",    fallback=3)
BACKOFF    = _cfg.getfloat("API_RETRY",  "backoff",    fallback=0.5)
DEMO       = False

def _timestamp_ms() -> int:
    return int(time.time() * 1000)

def parseParam(params: dict) -> str:
    """Собирает query string, сортируя ключи по алфавиту."""
    return "&".join(f"{k}={params[k]}" for k in sorted(params))

def _klines_cache_key(fn, self, *args, **kwargs) -> str:
    """Универсальный key_builder для get_klines."""
    symbol   = args[0]
    interval = kwargs.get("interval", args[1] if len(args)>1 else "1m")
    limit    = kwargs.get("limit",    args[3] if len(args)>3 else args[2] if len(args)>2 else 14)
    return f"klines:{symbol}:{interval}:{limit}"

# ────────────────────────────────────────────────────────────────────────────────
class CachingBingXClient(MarketClient):
    """
    BingX-клиент с кэшированием и безопасными ретраями.
    Полностью совместим с вашим оригинальным BingXClient.
    """

    def __init__(self, api_key: str, secret_key: str):
        super().__init__(api_key, secret_key)
        self.api_key     = api_key
        self.secret_key  = secret_key
        self.market_type = MarketEnum.BINGX
        self.base_url    = (
            "https://open-api.bingx.com"
            if not DEMO else
            "https://open-api-vst.bingx.com"
        )
        # единая aiohttp-сессия для всех запросов
        self._session    = ClientSession()
        # используем ваш общий логгер
        self.logger      = logger

    def _headers(self) -> dict:
        return {"X-BX-APIKEY": self.api_key}

    async def _safe_get(
        self,
        url: str,
        retries: int = RETRIES,
        backoff: float = BACKOFF
    ) -> dict:
        """
        Безопасный GET с retry/backoff.
        """
        last_exc = None
        for attempt in range(1, retries + 1):
            try:
                async with self._session.get(url, headers=self._headers()) as resp:
                    data = await resp.json()
                    if data.get("code", 0) != 0:
                        raise Exception(f"[BingX API] {data}")
                    return data
            except (ClientError, asyncio.TimeoutError, Exception) as e:
                last_exc = e
                self.logger.warning(f"[safe_get] {attempt}/{retries} failed: {e}")
                if attempt == retries:
                    break
                await asyncio.sleep(backoff * (2 ** (attempt - 1)))
        # все попытки провалены
        raise last_exc

    @cached(
        ttl=KLINES_TTL,
        cache=Cache.MEMORY,
        key_builder=_klines_cache_key
    )
    async def get_klines(
        self,
        symbol: str,
        interval: str = "1m",
        start:  int | None = None,
        limit:  int = 14
    ) -> list[TickerKline]:
        """
        Кешируем исторические свечи на KLINES_TTL секунд.
        """
        path = "/openApi/swap/v3/quote/klines"
        if start is None:
            start = _timestamp_ms() - 60_000
        params = {"symbol": symbol, "interval": interval, "limit": str(limit)}
        url = f"{self.base_url}{path}?{parseParam(params)}"
        data = await self._safe_get(url)
        return [TickerKline.model_validate(item) for item in data["data"]]

    @cached(
        ttl=PRICE_TTL,
        cache=Cache.MEMORY,
        key_builder=lambda fn, self, symbol: f"price:{symbol}"
    )
    async def get_ticker_price(self, symbol: str) -> float:
        """
        Кешируем цену на PRICE_TTL секунд.
        """
        path = "/openApi/swap/v2/quote/price"
        params = {"symbol": symbol, "timestamp": _timestamp_ms()}
        url = f"{self.base_url}{path}?{parseParam(params)}"
        data = await self._safe_get(url)
        return float(data["data"]["price"])

    async def close(self):
        """Корректно закрываем aiohttp-сессию."""
        if hasattr(self, "_session") and not self._session.closed:
            await self._session.close()

    async def clear_cache(self):
        """Очистить весь кэш вручную (опционально)."""
        await Cache.MEMORY.clear()
