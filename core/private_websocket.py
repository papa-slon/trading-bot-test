"""
core/private_websocket.py

Приватный поток @order: пуш-уведомления об исполнении/отмене ордеров.
Использует **уже существующий event-loop** (target_loop), поэтому не
создаёт собственный — это предотвращает конфликт greenlet-spawn в
SQLAlchemy.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import json
import logging
import threading
import uuid
from typing import Callable

import websocket

logger = logging.getLogger(__name__)

URL = "wss://open-api-swap.bingx.com/swap-market?listenKey={}"


class BingxPrivateWebsocket:
    def __init__(
        self,
        listen_key: str,
        on_order: Callable[[dict], None],
        target_loop: asyncio.AbstractEventLoop,
    ) -> None:
        self.url = URL.format(listen_key)
        self.on_order = on_order
        self.loop = target_loop            # ← общий loop из TradingBot
        self.ws = None

    # ───────────── WebSocket callbacks ───────────── #

    def on_open(self, ws):  # noqa: D401
        sub = {
            "id": str(uuid.uuid4()),
            "reqType": "sub",
            "dataType": "@order",
        }
        ws.send(json.dumps(sub))
        logger.info("[PrivateWS] subscribed @order")

    def on_data(self, ws, message, *_):
        try:
            buf = gzip.GzipFile(fileobj=io.BytesIO(message), mode="rb").read()
            data = json.loads(buf.decode())

            # Пробрасываем обработку в общий loop
            asyncio.run_coroutine_threadsafe(self.on_order(data), self.loop)

        except Exception as exc:  # noqa: BLE001
            logger.error("[PrivateWS] parse error: %s", exc, exc_info=True)

    def on_error(self, ws, error):
        logger.error("[PrivateWS] %s", error)

    def on_close(self, ws, code, msg):  # noqa: N803
        logger.warning("[PrivateWS] closed %s – %s", code, msg)

    # ───────────── Public API ───────────── #

    def start(self):
        """Запускает WebSocket в отдельном daemon-потоке"""
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self.on_open,
            on_data=self.on_data,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        threading.Thread(target=self.ws.run_forever, daemon=True).start()