from __future__ import annotations

import asyncio
import hmac
import json
import logging
import time
from hashlib import sha256
from typing import Any, Dict, List, Optional

from aiohttp import ClientSession, ClientTimeout, TCPConnector

from core.rate_limiter import RateLimiter
from market_api.core import MarketClient, OrderModel, PositionModel
from sqlalchemy_enums import MarketEnum

logger = logging.getLogger(__name__)

# ────────────────────────── КОНСТАНТЫ ────────────────────────────
DEMO: bool = False                       # True → open-api-vst
DEFAULT_RPS: int = 2                     # 2 req/s = 120 req/min
TIMEOUT: ClientTimeout = ClientTimeout(total=10)

PUBLIC_PREFIXES = (
    "/openApi/swap/v2/quote/",
    "/openApi/swap/v3/quote/klines",
    "/openApi/swap/market/",
)

# ────────────────────────── УТИЛИТЫ ──────────────────────────────
def _timestamp_ms() -> str:
    return str(int(time.time() * 1000))


def _query_str(params: Dict[str, Any]) -> str:
    return "&".join(f"{k}={params[k]}" for k in sorted(params) if params[k] is not None)



def _sign(secret: str, qs: str) -> str:
    return hmac.new(secret.encode(), qs.encode(), sha256).hexdigest()


def parseParam(paramsMap: Dict[str, Any]) -> str:
    """
    Утилита для старого TradingView-формата:
    добавляет timestamp и возвращает query-строку.
    """
    sortedKeys = sorted(paramsMap)
    paramsStr = "&".join(f"{k}={paramsMap[k]}" for k in sortedKeys)
    return (
        f"{paramsStr}&timestamp={_timestamp_ms()}"
        if paramsStr
        else f"timestamp={_timestamp_ms()}"
    )

# ────────────────────────── ИСКЛЮЧЕНИЯ ───────────────────────────
class AuthError(Exception):
    """BingX code 100413 — неверный API-ключ."""

# ─────────────────────────── КЛИЕНТ ──────────────────────────────
class BingXClient(MarketClient):  # type: ignore[misc]
    """
    Полнофункциональный клиент BingX Swap-V2/V3:

    • singleton-паттерн `instance()/reset_singleton()`
    • постоянная aiohttp-сессия + TCPConnector(limit=20)
    • RPS-лимитер, экспоненциальные retry, кеш маржи
    • потокобезопасная смена ключей (`set_credentials`)
    • правильная подпись для GET/POST (x-www-form-urlencoded)
    • адаптер `send_request()` для устаревших TV-сигналов
    """

    _singleton: "BingXClient | None" = None

    # ---------- factory ----------
    @classmethod
    def instance(
        cls,
        api_key: str,
        secret_key: str,
        *,
        rps: int = DEFAULT_RPS,
    ) -> "BingXClient":
        if cls._singleton is None:
            cls._singleton = cls(api_key, secret_key, rps=rps)
        return cls._singleton

    @classmethod
    async def reset_singleton(cls) -> None:
        if cls._singleton is not None:
            try:
                await cls._singleton.aclose()
            finally:
                cls._singleton = None

    # ---------- ctor ----------
    def __init__(self, api_key: str, secret_key: str, *, rps: int) -> None:
        super().__init__(api_key, secret_key)
        self.api_key = api_key
        self.secret_key = secret_key
        self.market_type = MarketEnum.BINGX

        self.base_url = (
            "https://open-api.bingx.com"
            if not DEMO
            else "https://open-api-vst.bingx.com"
        )

        self._connector = TCPConnector(limit=20, ttl_dns_cache=60)
        self._session = ClientSession(
            headers={
                "X-BX-APIKEY": self.api_key,
                "User-Agent": "trading-bot/1.0",
            },
            connector=self._connector,
            timeout=TIMEOUT,
            json_serialize=lambda d: json.dumps(d, separators=(",", ":")),
        )
        self._limiter = RateLimiter(rps=rps)
        self._margin_cache: Dict[str, tuple[float, float]] = {}
        self._lock = asyncio.Lock()

    # ---------- helpers ----------
    async def set_credentials(self, api_key: str, secret_key: str) -> None:
        async with self._lock:
            self.api_key = api_key
            self.secret_key = secret_key
            self._session.headers["X-BX-APIKEY"] = api_key

    # ---------- низкоуровневый запрос ----------
    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
        retries: int = 2,
    ) -> Dict[str, Any]:
        """
        Универсальный REST-вызов.
        • Public   → параметры в query, без подписи.
        • Private:
            • GET/DELETE → параметры в query, +timestamp, +signature
            • POST/PUT   → параметры в теле (x-www-form-urlencoded),
              подпись рассчитывается по телу, туда же добавляются
              timestamp и signature.
        """
        await self._limiter.acquire()

        is_public = path.startswith(PUBLIC_PREFIXES)
        url = f"{self.base_url}{path}"

        # ----------------------- PRIVATE GET / DELETE -----------------------
        if not is_public and method in {"GET", "DELETE"}:
            params = params or {}
            params["timestamp"] = _timestamp_ms()
            qs = _query_str(params)
            #params["signature"] = _sign(self.secret_key, qs)
            url += f"?{_query_str(params)}"
            url+=f"&signature={_sign(self.secret_key, qs)}"
            body = None
            headers = self._session.headers

        # ----------------------- PRIVATE POST / PUT ------------------------
        elif not is_public and method in {"POST", "PUT"}:
            payload = payload or {}
            payload["timestamp"] = _timestamp_ms()
            qs = _query_str(payload)  # строка БЕЗ signature
            #payload["signature"] = _sign(self.secret_key, qs)
            body = _query_str(payload)  # x-www-form-urlencoded
            headers = {
                **self._session.headers,
                "Content-Type": "application/x-www-form-urlencoded",
            }
            url+=f"?signature={_sign(self.secret_key, qs)}"  # query-параметров нет

        # ---------------------------- PUBLIC -------------------------------
        else:
            qs = _query_str(params or {})
            url += f"?{qs}" if qs else ""
            body = None
            headers = self._session.headers

        # --------------------------- HTTP вызов ----------------------------
        for attempt in range(retries + 1):
            try:
                async with self._session.request(
                    method,
                    url,
                    data=body,
                    headers=headers,
                ) as resp:
                    js = await resp.json(content_type=None)
            except (asyncio.TimeoutError, ConnectionError) as exc:
                if attempt == retries:
                    raise
                logger.warning("HTTP %s — retry %d/%d", exc, attempt + 1, retries)
                await asyncio.sleep(2 ** attempt)
                continue

            if js.get("code") == 100410 and attempt < retries:  # rate-limit
                await asyncio.sleep(2 ** attempt)
                continue
            if js.get("code") == 100413:  # bad key
                raise AuthError(js.get("msg", "invalid key"))
            logger.info(f"Request for url {url} - success")
            return js
        raise RuntimeError("unreachable")

    async def aclose(self) -> None:
        await self._session.close()
        await self._connector.close()

    # ────────────────────────  CONTRACT RULES  ──────────────────────────
    _symbol_rules_cache: Dict[str, Dict[str, float]] = {}

    async def _get_symbol_rules(self, symbol: str) -> Dict[str, float]:
        """
        Возвращает {'stepSize': 0.01, 'minNotional': 2.0, 'pricePrecision': 2, …}.
        Кеширует ответ на 1 час (в рамках жизни процесса).
        """
        if symbol in self._symbol_rules_cache:
            return self._symbol_rules_cache[symbol]

        js = await self._request(
            "GET",
            "/openApi/swap/v2/quote/contracts",
            params={"symbol": symbol},
        )
        if js.get("code") != 0 or not js.get("data"):
            # fallback — дефолтные лимиты BingX
            rules = {"stepSize": 0.01, "minNotional": 2.0}
            self._symbol_rules_cache[symbol] = rules
            return rules

        data = js["data"][0] if isinstance(js["data"], list) else js["data"]
        rules = {
            "stepSize": float(data.get("stepSize", 0.01)),
            "minNotional": float(data.get("minNotional", 2.0)),
            "pricePrecision": int(data.get("pricePrecision", 5)),
            "qtyPrecision": int(data.get("quantityPrecision", 6)),
        }
        self._symbol_rules_cache[symbol] = rules
        return rules

    async def normalize_qty(
        self, symbol: str, qty: float, price: float
    ) -> float:
        """
        Корректирует qty вверх:
        • кратно stepSize,
        • номинал ≥ minNotional.
        Вернёт 0.0, если даже minNotional > баланс → такой ордер следует пропустить.
        """
        rules = await self._get_symbol_rules(symbol)
        step = rules["stepSize"]
        notional_min = rules["minNotional"]

        # округление qty вверх до ближайшего шага
        from math import ceil
        qty = ceil(qty / step) * step

        # проверка минимального номинала
        if price * qty < notional_min:
            qty = ceil(notional_min / price / step) * step

        # финальное округление до qtyPrecision
        prec = rules.get("qtyPrecision", 6)
        return round(qty, prec)


    # ──────────────────────── PUBLIC QUOTE ────────────────────────
    async def get_ticker_price(self, symbol: str) -> float:
        js = await self._request(
            "GET", "/openApi/swap/v2/quote/price", params={"symbol": symbol}
        )
        if js.get("code") != 0:
            raise Exception(js)
        return float(js["data"]["price"])

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 200,
    ) -> List[List[str | float]]:
        js = await self._request(
            "GET",
            "/openApi/swap/v3/quote/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
        )
        if js.get("code") != 0:
            raise Exception(js)
        return js["data"]

    async def get_all_prices(self, symbol_list: List[str]):
        d = {}
        url = "/openApi/swap/v1/ticker/price"
        for s in symbol_list:
            js = await self._request("GET", url, params={"symbol": s})
            d[s] = float(js["data"]["price"])
        return d


    # ─────────────────────── USER / TRADE ─────────────────────────
    async def get_available_margin(self, *, force: bool = False) -> float:
        now = time.monotonic()
        cached, ts = self._margin_cache.get(self.api_key, (None, 0.0))
        if cached is not None and not force and now - ts < 30:
            return cached

        js = await self._request("GET", "/openApi/swap/v2/user/balance")
        if js.get("code") != 0:
            raise Exception(js)

        raw = js["data"]
        acc = (
            next((a for a in raw if a.get("asset") == "USDT"), raw[0])
            if isinstance(raw, list)
            else raw.get("balance", raw)
        )
        margin = float(
            acc.get("availableBalance")
            or acc.get("available")
            or acc.get("availableMargin")
            or acc.get("walletBalance")
        )
        self._margin_cache[self.api_key] = (margin, now)
        return margin

    async def get_balance(self) -> float:
        return await self.get_available_margin(force=True)

    async def get_positions(self, symbol: str) -> PositionModel:
        js = await self._request(
            "GET", "/openApi/swap/v2/user/positions", params={"symbol": symbol}
        )
        if not js["data"]:
            return PositionModel.model_validate(
                {"symbol": symbol, "availableAmt": 0.0, "avgPrice": 0.0}
            )
        return PositionModel.model_validate(js["data"][0])

    async def get_open_orders(self, symbol: str) -> List[OrderModel]:
        js = await self._request(
            "GET", "/openApi/swap/v2/trade/openOrders", params={"symbol": symbol}
        )
        try:
            return [OrderModel.model_validate(o) for o in js["data"]["orders"]]
        except:
            logger.warning(f"Error on get_open_orders, response: {js}")

    # --------------------------- TRADE ----------------------------
    async def place_order(
        self,
        symbol: str,
        side: str,
        order_type: str,
        *,
        pos_side: str = "LONG",
        quantity: float = 0.0,
        price: float = 0.0,
        time_in_force: str = "GTC",
        extra: Dict = None,
    ) -> int:
        body: Dict[str, Any] = {
            "symbol": symbol,
            "side": side,
            "type": order_type,
            "positionSide": pos_side,
            "quantity": quantity,
            "timeInForce": time_in_force,
        }

        if extra:
            for k, v in extra.items():
                body[k] = v

        if order_type == "LIMIT":
            body["price"] = price

        js = await self._request(
            "POST", "/openApi/swap/v2/trade/order", payload=body
        )
        if js.get("code") != 0:
            raise RuntimeError(f"order not accepted: {js}")

        data = js.get("data", {})
        return int(
            data.get("orderId")
            or data.get("order", {}).get("orderId", 0)
            or 0
        )

    async def cancel_order(self, symbol: str, order_id: int) -> bool:
        await self._request(
            "DELETE",
            "/openApi/swap/v2/trade/order",
            params={"symbol": symbol, "orderId": order_id},
        )
        return True

    async def cancel_all_orders(self, symbol: str) -> bool:
        await self._request(
            "DELETE",
            "/openApi/swap/v2/trade/allOpenOrders",
            params={"symbol": symbol},
        )
        return True

    async def close_position(self, symbol: str, pos_side: str = "LONG") -> bool:
        await self._request(
            "POST",
            "/openApi/swap/v2/trade/closePosition",
            params={"symbol": symbol, "positionSide": pos_side},
        )
        return True

    async def close_all_positions(self) -> bool:
        await self._request("POST", "/openApi/swap/v2/trade/closeAllPositions")
        return True

    # ───────────────────────── batch-енд-поинты ──────────────────────────

    async def place_batch_orders(
            self,
            orders: List[Dict[str, Any]],
    ) -> List[int]:
        """
        POST /openApi/swap/v2/trade/batchOrders
        Разом ставит ≤ 5 ордеров (лимит BingX).
        Возвращает orderId-ы в том же порядке, что и во входном списке.
        """
        if not orders:
            return []

        order_ids: List[int] = []
        chunk_size = 5  # лимит биржи
        for i in range(0, len(orders), chunk_size):
            chunk = orders[i: i + chunk_size]

            # тело запроса — x-www-form-urlencoded
            body = {"batchOrders": json.dumps(chunk, separators=(",", ":"))}
            js = await self._request(
                "POST",
                "/openApi/swap/v2/trade/batchOrders",
                payload=body,
            )
            if js.get("code") != 0:
                raise RuntimeError(f"batchOrders rejected: {js}")

            data = js.get("data", {}).get("orders", [])
            chunk_ids = [
                int(o.get("orderId") or o.get("order", {}).get("orderId", 0))
                for o in data
            ]
            if len(chunk_ids) != len(chunk):
                raise RuntimeError("length mismatch in batchOrders response")
            order_ids.extend(chunk_ids)

        return order_ids

    
    # ─────────────────────  BATCH-CANCEL  ─────────────────────────────
    async def cancel_batch_orders(self, ids: list[int], symbol: str) -> None:

        if not ids:  # нечего снимать
            return

        # — 1. формируем строку ID в формате, который ждёт BingX ——–––
        order_id_list = "[" + ",".join(str(i) for i in ids) + "]"

        # — 2. отправляем запрос через общую обёртку _request() ————–
        js = await self._request(
            "DELETE",
            "/openApi/swap/v2/trade/batchOrders",
            params={
                "orderIdList": order_id_list,
                "symbol": symbol,
            },
        )

        # — 3. проверяем ответ биржи ————————————————————————————
        if js.get("code") != 0:
            raise RuntimeError(f"batch cancel rejected: {js}")

    # ───────────────── legacy send_request ────────────────────────
    async def send_request(
        self,
        method: str,
        url: str,
        params_str: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        path = url[len(self.base_url) :]
        p_dict: Dict[str, Any] = (
            {kv.split("=", 1)[0]: kv.split("=", 1)[1] for kv in params_str.split("&") if kv}
            if params_str
            else {}
        )
        return await self._request(method, path, params=p_dict, payload=payload)

