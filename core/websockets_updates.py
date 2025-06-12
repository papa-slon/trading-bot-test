from __future__ import annotations
import asyncio, configparser, gzip, io, json, logging, random, websockets
from typing import Callable, Iterable, List
logger = logging.getLogger(__name__)

BASE_WS = "wss://open-api-swap.bingx.com/swap-market"

_cfg = configparser.ConfigParser(); _cfg.read("config.ini")
symb_list: List[str] = sorted({s.strip().upper() for s in _cfg.get("BOT", "SYMBOLS", fallback="BTC-USDT").split(",")})

def run_async_parallel(coro, args=()):
    asyncio.get_running_loop().create_task(coro(*args))

class _SymbolWS:
    def __init__(self, symbols: Iterable[str], on_price: Callable[[str, float, float], None]):
        self._symbols = sorted({s.upper() for s in symbols})
        self._on_price = on_price
        self._ws: websockets.WebSocketClientProtocol | None = None
        self._stop = asyncio.Event()

    # ── connect ──────────────────────────────────────────
    async def _connect(self):
        url = f"{BASE_WS}?symbol=" + ",".join(self._symbols)
        self._ws = await websockets.connect(
            url,
            ping_interval=None,        # отключаем встроенный
            max_size=2 ** 20,
        )
        logger.info("WS connected <%s>", ",".join(self._symbols))

    # ── listen loop ──────────────────────────────────────
    async def _listen(self):
        assert self._ws
        async for raw in self._ws:
            # бинарный gzip
            if isinstance(raw, bytes):
                try:
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read().decode()
                except OSError:
                    continue
            # текстовый ping
            if raw in ("Ping", "ping"):
                await self._ws.send("Pong")
                continue

            # JSON-сообщение
            try:
                js = json.loads(raw)
            except json.JSONDecodeError:
                continue

            # heartbeat {"ping": ts}
            if "ping" in js:
                await self._ws.send(json.dumps({"pong": js["ping"]}))
                continue

            sym = js.get("s") or js.get("symbol")
            p   = js.get("c") or js.get("price")
            ts  = js.get("t") or js.get("timestamp")
            if sym and p is not None:
                try:
                    self._on_price(sym.upper(), float(p), float(ts or 0))
                except Exception:
                    logger.exception("price callback")

    # ── ручной пинг каждые 20 с ─────────────────────────
    async def _pinger(self):
        while not self._stop.is_set():
            try:
                if self._ws and self._ws.open:
                    await self._ws.ping()
            except Exception:
                pass
            await asyncio.sleep(20)

    # ── основной цикл ────────────────────────────────────
    async def run_forever(self):
        delay = 2.0
        while not self._stop.is_set():
            try:
                await self._connect()
                ping_task = asyncio.create_task(self._pinger(), name="bx-ping")
                await self._listen()             # вернётся при закрытии WS
            except websockets.ConnectionClosedError as e:
                logger.warning("WS closed: %s", e)
                delay = min(delay * 1.5, 30)
            except Exception:
                logger.exception("WS unknown error")
                delay = min(delay * 1.5, 30)
            finally:
                # остановим пинг-таск, если есть
                if 'ping_task' in locals():
                    ping_task.cancel()

            if not self._stop.is_set():
                logger.warning("reconnect in %.1fs", delay)
                await asyncio.sleep(delay)

    # ── graceful close ───────────────────────────────────
    async def aclose(self):
        self._stop.set()
        if self._ws and not self._ws.closed:
            await self._ws.close(code=1000)

class BingxWebsocket(_SymbolWS):
    def __init__(self, symbols: Iterable[str], cb: Callable[[str,float],None]):
        super().__init__(symbols, lambda s,p,_t: cb(s,p)); self._task=None
    def start(self):  # старый интерфейс
        if not self._task or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self.run_forever())
    def stop(self):
        if self._task and not self._task.done():
            asyncio.get_running_loop().create_task(self.aclose())
