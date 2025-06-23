import asyncio
import json
import logging
import threading
import uuid
from typing import List, Dict, Any

from market_api.core import MarketClient
from core.private_websocket import BingxPrivateWebsocket
from models import (
    add_order,
    add_slot,
    can_open_extra,
    close_slot,
    count_active_slots,
    get_active_slot,
    get_api_by_key,
    get_averaging_orders,
    get_tp_order,
    increment_avg_count,
    tetris_move,
    toggle_order,
    update_tp,
)
from sqlalchemy_enums import (
    SideEnum,
    OrderTypeEnum,
    PositionSideEnum,
    NoteEnum,
    ReasonEnum,
)
from services.price_cache import PriceCache
from services.price_feed import PriceFeed

logger = logging.getLogger(__name__)


def _safe_float(val, default: float = 0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


class TradingBot:
    """
    Логика бота: вход, усреднения, TP, тетрис-перемещения.
    """

    def __init__(
        self,
        client: MarketClient,
        symbols: List[str],
        take_profit_percent: float,
        initial_entry_percent: float,
        averaging_levels: int,
        averaging_percents: List[float],
        averaging_volume_percents: List[float],
        use_balance_percent: float = 100.0,
        leverage: int = 3,
        max_symbols: int = 6,
        trailing_stop_percent: float = 0.6,
        activation: float = 0.6,
        base_stop: float = 0.6,
        socketio=None,
    ):
        self.client = client
        self.symbol_list = symbols
        PriceFeed.instance().subscribe(self.symbol_list, self.on_price)
        self.take_profit_percent = take_profit_percent
        self.initial_entry_percent = initial_entry_percent
        self.averaging_levels = averaging_levels
        self.averaging_percents = averaging_percents
        self.averaging_volume_percents = averaging_volume_percents
        self.use_balance_percent = use_balance_percent
        self.leverage = leverage
        self.max_symbols = max_symbols
        self.socketio = socketio
        self.logger = logger
        self.api_instance = None
        self.running = False
        self.loop: asyncio.AbstractEventLoop | None = None
        self._private_ws: BingxPrivateWebsocket | None = None
        self._keepalive_task: asyncio.Task | None = None
        self._current_leverage: Dict[str, int] = {}
        self.trailing_stop = trailing_stop_percent/100
        self.activation_pr = activation/100
        self.base_stop = base_stop/100


    # ──────────────────────────── Служебные методы ─────────────────────────── #

    def _emit(self, event: str, data: dict) -> None:
        if self.socketio:
            self.socketio.emit(event, data)
        self.logger.info("[emit] %s → %s", event, data)

    async def set_api(self) -> None:
        self.api_instance = await get_api_by_key(
            self.client.api_key, self.client.secret_key
        )

    # ────────────────────────── Работа с ордерами ─────────────────────────── #
    async def on_price(self, symbol: str, price: float, ts: float | None = None) -> None:
        try:
            await PriceCache.update(symbol, price)
            self.logger.debug(f"[on_price] Цена обновлена: {symbol}={price}")
        except Exception as exc:
            self.logger.error(f"[on_price] Ошибка при обновлении цены: {exc}", exc_info=True)

    async def execute_order(
        self,
        symbol: str,
        side: SideEnum,
        order_type: OrderTypeEnum,
        position_side: PositionSideEnum,
        quantity: float,
        price: float,
        note: NoteEnum,
        system_id: str,
        reason: ReasonEnum,
        stage: int,
        extra: Dict[str, Any] = None,
        add_to_db: bool = True,
    ) -> str:
        if extra is None:
            extra = {}
        order_id = await self.client.place_order(
            symbol=symbol,
            side=side.value,
            order_type=order_type.value,
            pos_side=position_side.value,
            quantity=quantity,
            price=price,
            extra = extra
        )
        if add_to_db:
            await add_order(
                order_id=order_id,
                order_type=order_type,
                position_side=position_side,
                side=side,
                note=note,
                price=price,
                quantity=quantity,
                position=symbol,
                api_id=self.api_instance.pub_id,
                system_id=system_id,
                reason=reason,
                stage=stage,
            )
        return order_id

    # ────────────────────────── Получение цены ────────────────────────────── #

    async def _safe_get_ticker_price(
        self,
        symbol: str,
        retries: int = 5,
        base_delay: float = 0.5,
        max_cache_age: float = 60.0,
    ) -> float:
        cached = await PriceCache.get(symbol, max_age=max_cache_age)
        if cached is not None and cached > 0:
            return cached

        # ① одним запросом берём ВСЕ цены
        bulk = await self.client.get_all_prices(self.symbol_list)
        await PriceCache.bulk_update(bulk)

        price = bulk.get(symbol)
        if price:
            return price

        # ② если вдруг нужного символа нет → старая точечная логика
        return await self.client.get_ticker_price(symbol)

    async def safe_get_positions(self, symbol: str, retries: int = 3) -> list:
        for attempt in range(1, retries + 1):
            try:
                return await self.client.get_positions(symbol)
            except Exception as exc:
                if attempt == retries:
                    raise
                await asyncio.sleep(0.4 * 2 ** (attempt - 1))

    async def _report_status(self, symbol: str) -> None:
        try:
            price = await self._safe_get_ticker_price(symbol)
            pos = await self.safe_get_positions(symbol)
            self._emit("bot_status", {"symbol": symbol, "price": price, "positions": pos})
        except Exception as exc:
            self.logger.error("[_report_status] %s", exc, exc_info=True)


    # ───────────────────────────── Открытие позиции ─────────────────────────── #

    async def open_position(
        self, symbol: str, reason: ReasonEnum, stage: int | None = None
    ) -> None:
        try:
            cached_leverage = self._current_leverage.get(symbol)
            if cached_leverage != self.leverage:
                await self.client.set_leverage(symbol, self.leverage)
                self._current_leverage[symbol] = self.leverage

            # 1) Дубликат монеты
            if await get_active_slot(self.api_instance.pub_id, symbol):
                self._emit("bot_log", {"msg": f"{symbol} уже в работе"})
                return

            # 2) Проверка лимитов и этапов
            can_open, stage = await can_open_extra(self.api_instance.pub_id, reason)
            if not can_open:
                self._emit("bot_log", {"msg": "Достигнут лимит слотов"})


            # 3) Баланс
            balance = _safe_float(await self.client.get_balance())
            effective_balance = balance * self.leverage
            if effective_balance <= 0:
                self._emit("bot_log", {"msg": "Нет свободного баланса"})
                return

            base_notional = effective_balance * self.use_balance_percent / 100
            entry_usdt = base_notional * self.initial_entry_percent / 100

            # 4) Актуальная цена (кэш → REST)
            cur_price = await PriceCache.get(symbol, max_age=30.0)
            if cur_price is None:
                cur_price = await self.client.get_ticker_price(symbol)
                await PriceCache.update(symbol, cur_price)

            if cur_price is None or cur_price <= 0:
                self._emit(
                    "bot_log",
                    {"msg": f"Некорректная цена {cur_price} для {symbol}. Операция прервана."},
                )
                return

            raw_qty_entry = entry_usdt / cur_price
            qty_entry = await self.client.normalize_qty(symbol, raw_qty_entry, cur_price)
            if qty_entry == 0.0:
                self._emit("bot_log", {"msg": f"Слишком маленький баланс для {symbol}"})
                await close_slot(self.api_instance.pub_id, symbol)
                return
            system_id = str(uuid.uuid4())

            await add_slot(self.api_instance.pub_id, symbol, reason, stage)
            stop_loss_dict = {
                "type": "STOP_MARKET",
                "stopPrice": (1+self.base_stop)*cur_price,
            }
            try:
                await self.execute_order(
                    symbol,
                    SideEnum.SELL,
                    OrderTypeEnum.TRAILING_TP_SL,
                    PositionSideEnum.SHORT,
                    qty_entry,
                    0.0,
                    NoteEnum.TAKE_PROFIT,
                    system_id,
                    reason,
                    stage,
                    extra = {
                        "priceRate": self.trailing_stop,
                        "activationPrice": (1 - self.activation_pr)*cur_price,
                        "stopLoss": stop_loss_dict,
                    }
                )
            except Exception as exc:
                await close_slot(self.api_instance.pub_id, symbol)
                logger.warning("Failed to open %s: %s", symbol, exc, exc_info=True)
                return

            tp_price = round(cur_price * (1 + self.take_profit_percent / 100), 5)
            batch_orders: List[dict] = []
            metas: List[dict] = []
            # — TP —
            """batch_orders.append(
                {
                    "symbol": symbol,
                    "type": "TRAILING_TP_SL",
                    "side": "SELL",
                    "positionSide": "SHORT",
                    "quantity": qty_entry,
                    "timeInForce": "GTC",
                }
            )
            metas.append(
                {
                    "order_type": OrderTypeEnum.TRAILING_TP_SL,
                    "position_side": PositionSideEnum.SHORT,
                    "side": SideEnum.SELL,
                    "note": NoteEnum.TAKE_PROFIT,
                    "price": cur_price,
                    "quantity": qty_entry,
                }
            )"""

            # — averaging —
            if self.averaging_levels > 0:
                for lvl in range(self.averaging_levels):
                    d_pct = self.averaging_percents[lvl]
                    v_pct = self.averaging_volume_percents[lvl]
                    price = round(cur_price * (1 - d_pct / 100), 5)
                    raw_qty = effective_balance * v_pct / 100 / price
                    qty = await self.client.normalize_qty(symbol, raw_qty, price)
                    stop_loss_dict = {
                        "type": "STOP_MARKET",
                        "stopPrice": (1 + self.base_stop) * price,
                    }
                    if qty == 0.0:
                        continue  # пропускаем уровень, если не проходит

                    batch_orders.append(
                        {
                            "symbol": symbol,
                            "type": "TRAILING_TP_SL",
                            "side": "SELL",
                            "positionSide": "SHORT",
                            "quantity": qty,
                            "price": price,
                            "timeInForce": "GTC",
                            "priceRate": self.trailing_stop,
                            "activationPrice": (1 - self.activation_pr) * price,
                            "stopLoss": stop_loss_dict,
                        }
                    )
                    metas.append(
                        {
                            "order_type": OrderTypeEnum.TRAILING_TP_SL,
                            "position_side": PositionSideEnum.SHORT,
                            "side": SideEnum.SELL,
                            "note": NoteEnum.AVERAGING,
                            "price": price,
                            "quantity": qty,
                        }
                    )

            # — отправка batch-запроса —
            order_ids = await self.client.place_batch_orders(batch_orders)

            # — запись в БД —
            for oid, meta in zip(order_ids, metas):
                await add_order(
                    order_id=oid,
                    order_type=meta["order_type"],
                    position_side=meta["position_side"],
                    side=meta["side"],
                    note=meta["note"],
                    price=meta["price"],
                    quantity=meta["quantity"],
                    position=symbol,
                    api_id=self.api_instance.pub_id,
                    system_id=system_id,
                    reason=reason,
                    stage=stage,
                )

            await self._report_status(symbol)

        except Exception as exc:
            self.logger.error("[open_position] %s", exc, exc_info=True)
            self._emit("bot_error", {"error": str(exc)})

    # ────────────────── Усреднения и пересчёт TP ──────────────────
    async def place_averaging_orders(
            self,
            symbol: str,
            base_price: float,
            base_notional: float,
            reason: ReasonEnum,
            stage: int,
            system_id: str,
    ) -> None:
        if self.averaging_levels <= 0:
            return

        orders, metas = [], []

        balance = _safe_float(await self.client.get_balance())
        effective_balance = balance * self.leverage

        for lvl in range(self.averaging_levels):
            d_pct = self.averaging_percents[lvl]
            v_pct = self.averaging_volume_percents[lvl]
            price = round(base_price * (1 - d_pct / 100), 5)

            raw_qty = effective_balance * v_pct / 100 / price
            qty = await self.client.normalize_qty(symbol, raw_qty, price)
            if qty == 0.0:
                continue

            orders.append(
                {
                    "symbol": symbol,
                    "type": "LIMIT",
                    "side": "BUY",
                    "positionSide": "LONG",
                    "quantity": qty,
                    "price": price,
                    "timeInForce": "GTC",
                }
            )
            metas.append(
                {
                    "order_type": OrderTypeEnum.LIMIT,
                    "position_side": PositionSideEnum.LONG,
                    "side": SideEnum.BUY,
                    "note": NoteEnum.AVERAGING,
                    "price": price,
                    "quantity": qty,
                }
            )

        ids = await self.client.place_batch_orders(orders)
        for oid, meta in zip(ids, metas):
            await add_order(
                order_id=oid,
                order_type=meta["order_type"],
                position_side=meta["position_side"],
                side=meta["side"],
                note=meta["note"],
                price=meta["price"],
                quantity=meta["quantity"],
                position=symbol,
                api_id=self.api_instance.pub_id,
                system_id=system_id,
                reason=reason,
                stage=stage,
            )

    async def _cancel_all_averaging(self, symbol: str) -> None:
        """
        Снимает все AVERAGING-ордера по `symbol` максимально быстро.

        Порядок:
        1) Мгновенно скрываем записи в БД → UI сразу «чистый».
        2) Пытаемся снять ровно эти ID одним batch-запросом.
        3) Если batch не прошёл (или ID > 20) — fallback на /allOpenOrders.
        4) Последний рубеж — параллельные одиночные cancel_order.
        """
        avgs = await get_averaging_orders(self.api_instance.pub_id, symbol)
        if not avgs:
            return

        # ── 1. мгновенно убираем из интерфейса ───────────────────────────
        await asyncio.gather(
            *(toggle_order(self.api_instance.pub_id, str(a.order_id)) for a in avgs),
            return_exceptions=True,
        )

        ids = [int(a.order_id) for a in avgs]

        # ── 2. одна попытка batch-cancel (≤20 ID) ─────────────────────────
        try:
            await self.client.cancel_batch_orders(ids, symbol)  # /trade/batchOrders
            return
        except Exception as exc:
            self.logger.warning(
                "[_cancel_all_averaging] batch failed → %s", exc, exc_info=True
            )

        # ── 3. запасной вариант — снять всё разом ────────────────────────
        try:
            await self.client.cancel_all_orders(symbol)  # /trade/allOpenOrders
            return
        except Exception as exc:
            self.logger.warning(
                "[_cancel_all_averaging] allOpenOrders failed → %s", exc, exc_info=True
            )

        # ── 4. крайний случай — параллельные одиночные ───────────────────
        await asyncio.gather(
            *(self.client.cancel_order(symbol, oid) for oid in ids),
            return_exceptions=True,
        )

    async def _recalc_tp_after_averaging(self, symbol: str) -> None:
        try:
            #pos = await self.safe_get_positions(symbol)
            #amt = _safe_float(pos.amount)

            positions = await self.safe_get_positions(symbol)
            if positions is None:
                return

            # результат бывает либо списком, либо единичным объектом
            if isinstance(positions, list):
                if not positions:  # список пуст
                    return
                pos = positions[0]
            else:  # уже PositionModel
                pos = positions

            amt = _safe_float(pos.amount)

            if amt <= 0:
                return

            entry = _safe_float(pos.avg_price)
            new_tp_price = round(entry * (1 + self.take_profit_percent / 100), 5)
            tp_order = await get_tp_order(self.api_instance.pub_id, symbol)

            if tp_order and tp_order.price == new_tp_price:
                return

            # ─── 1. если TP был, отменяем старый ─────────────────────────────
            if tp_order:
                try:
                    await self.client.cancel_order(symbol, tp_order.order_id)
                except Exception:
                    pass
                #await toggle_order(self.api_instance.pub_id, str(tp_order.order_id))

            # ─── 2. вычисляем reason, который запишем в новый TP ──────────────
            default_reason = tp_order.reason if tp_order else ReasonEnum.YELLOW  # ← новая строка

            # ─── 3. ставим новый TP ───────────────────────────────────────────
            new_oid = await self.execute_order(
                symbol,
                SideEnum.SELL,
                OrderTypeEnum.LIMIT,
                PositionSideEnum.LONG,
                amt,
                new_tp_price,
                NoteEnum.TAKE_PROFIT,
                tp_order.system_id if tp_order else str(uuid.uuid4()),
                default_reason,  # ← заменили аргумент
                tp_order.stage if tp_order else 1,
                add_to_db=False,
            )

            # ─── 4. если TP существовал раньше, обновляем запись в БД ─────────
            if tp_order:
                await update_tp(
                    tp_order.system_id,
                    self.api_instance.pub_id,
                    new_tp_price,
                    amt,
                    new_oid,
                )

            self._emit("bot_log", {"msg": f"TP пересчитан: {new_tp_price}"})
        except Exception as e:
            self.logger.error(f"[_recalc_tp_after_averaging] {e}", exc_info=True)
            self._emit("bot_error", {"error": str(e)})

    async def check_tp_filled(self, symbol: str) -> None:
        await self._process_open_orders(symbol)

    async def check_averaging_fills(self, symbol: str) -> None:
        await self._process_open_orders(symbol)


    # ─────────────────────────── Закрытие позиции ────────────────────────────
    async def close_position(self, symbol: str) -> None:
        tp_order = await get_tp_order(self.api_instance.pub_id, symbol)
        await self._cancel_all_averaging(symbol)
        if tp_order:
            try:
                await self.client.cancel_order(symbol, tp_order.order_id)
            except Exception:
                pass
            await toggle_order(self.api_instance.pub_id, str(tp_order.order_id))

        await self.client.close_position(symbol, pos_side=PositionSideEnum.LONG.value)
        await close_slot(self.api_instance.pub_id, symbol)
        if tp_order:
            await tetris_move(self.api_instance.pub_id, tp_order.stage, tp_order.reason)
        self._emit("bot_log", {"msg": f"Позиция {symbol} закрыта"})
        await self._report_status(symbol)

    # ───────────────────────── Фоновая проверка ───────────────────────────────
    async def _process_open_orders(self, symbol: str) -> None:
        """
        Один вызов get_open_orders → обработка и усреднений, и TP.
        """
        try:
            open_orders = await self.client.get_open_orders(symbol)
            live_ids = {
                str(o.order_id)
                for o in open_orders
                if getattr(o, "orderStatus", getattr(o, "status", "NEW"))
                not in ("FILLED", "CANCELED")
            }

            # ─── 1. усреднения ──────────────────────────────────────────────
            avgs = await get_averaging_orders(self.api_instance.pub_id, symbol)
            done_avgs = [a for a in avgs if str(a.order_id) not in live_ids]

            for a in done_avgs:
                await toggle_order(self.api_instance.pub_id, str(a.order_id))
                await increment_avg_count(self.api_instance.pub_id, symbol)

            if done_avgs:
                # пересчёт TP ставит новый лимит-продажу; старый tp_order
                # уже не актуален, поэтому завершаем цикл и ждём следующего
                await self._recalc_tp_after_averaging(symbol)
                await self._report_status(symbol)
                return  # ← ключевая строка

            # ─── 2. TP ──────────────────────────────────────────────────────
            tp_order = await get_tp_order(self.api_instance.pub_id, symbol)
            if not tp_order or str(tp_order.order_id) not in live_ids:
                # логика прежнего check_tp_filled (снятие усреднений,
                # закрытие позиции, tetris_move и т.д.)
                if tp_order:
                    await toggle_order(self.api_instance.pub_id, str(tp_order.order_id))
                await self._cancel_all_averaging(symbol)
                await close_slot(self.api_instance.pub_id, symbol)
                if tp_order:
                    await tetris_move(self.api_instance.pub_id, tp_order.stage, tp_order.reason)
                await self.close_position(symbol)
                self._emit("bot_log", {"msg": f"TP по {symbol} исполнен и все усредняющие ордера отменены"})

            # ─── 3. статус ─────────────────────────────────────────────────
            if done_avgs or (tp_order and str(tp_order.order_id) not in live_ids):
                await self._report_status(symbol)

        except Exception as exc:
            self.logger.error(f"[_process_open_orders] {exc}", exc_info=True)
            self._emit("bot_error", {"error": str(exc)})


    # ─────────────────── Приватный push-stream ─────────────────── #

    async def _start_private_stream(self) -> None:
        key = await self.client._get_listen_key()  # ← REST /user/auth/start
        # listen-key, callback-корутина, общий event-loop
        self._private_ws = BingxPrivateWebsocket(
            key,
            self._on_order_update,
            self.loop,
        )
        self._private_ws.start()

        # keep-alive каждые 30 мин
        async def _keepalive():
            while self.running:
                await asyncio.sleep(1800)
                await self.client._keepalive_listen_key()
        self._keepalive_task = asyncio.create_task(_keepalive())

    async def _stop_private_stream(self) -> None:
        if self._keepalive_task:
            self._keepalive_task.cancel()
        await self.client._close_listen_key()

    async def _on_order_update(self, msg: dict) -> None:
        """
        Обработка push-уведомления.
        msg пример:
        {
          "e":"ORDER_TRADE_UPDATE",
          "E":1699860020000,
          "o":{
             "s":"BTC-USDT","i":123,"X":"FILLED","o":"LIMIT",
             "S":"BUY","z":"0.002","ap":"34000","execType":"TRADE"
          }
        }
        """
        try:
            if msg.get("e") != "ORDER_TRADE_UPDATE":
                return
            order = msg["o"]
            symbol = order["s"]
            status = order["X"]             # FILLED / CANCELED / NEW …

            # если FILLED → сразу обновляем БД и расчёт TP / закрытие
            if status == "FILLED":
                oid = str(order["i"])
                await toggle_order(self.api_instance.pub_id, oid)

                # если ордер — AVERAGING → пересчёт TP
                if order["o"] == "LIMIT" and order["S"] == "BUY":
                    await increment_avg_count(self.api_instance.pub_id, symbol)
                    await self._recalc_tp_after_averaging(symbol)

                # если ордер — TAKE_PROFIT
                if order["o"] == "LIMIT" and order["S"] == "SELL":
                    await self._cancel_all_averaging(symbol)
                    await close_slot(self.api_instance.pub_id, symbol)
                    await self.close_position(symbol)
                    self._emit("bot_log",
                               {"msg": f"TP по {symbol} исполнен [push]"})
            # статус NEW/CANCELED можно игнорировать или логировать
        except Exception as exc:
            self.logger.error("[push-update] %s", exc, exc_info=True)


    # ───────────────────────── Фоновый цикл ──────────────────────────────── #

    async def _background(self) -> None:
        """
        Резервный опрос биржи раз в 1 час (3600 с).
        Основная логика приходит push-stream’ом.
        """
        while self.running:
            await asyncio.sleep(3600)  # ← было 45
            for sym in self.symbol_list:
                await self._process_open_orders(sym)

    def start(self) -> None:
        """
        • Создаём единый event-loop для TradingBot (self.loop).
        • Делаем его «текущим» в этом потоке, чтобы asyncio.get_running_loop()
          работал внутри PriceCache, SQLAlchemy и др.
        • Запускаем фоновый _background() в отдельном daemon-потоке.
        • На том же loop-е запускаем приватный WebSocket-поток @order.
        """
        if self.running:
            return
        self.running = True

        # 1) общий цикл для всех корутин бота
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)           # ← критичная строка

        # 2) фоновый цикл проверок (резервный openOrders раз в час)
        threading.Thread(
            target=lambda: self.loop.run_until_complete(self._background()),
            daemon=True,
        ).start()

        # 3) приватный поток BingX @order на том же loop
        asyncio.run_coroutine_threadsafe(
            self._start_private_stream(),
            self.loop,
        )

    def stop(self) -> None:
        """
        Останавливает фоновый цикл и закрывает приватный поток.
        """
        self.running = False
        if self.loop and not self.loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                self._stop_private_stream(),
                self.loop,
            )

