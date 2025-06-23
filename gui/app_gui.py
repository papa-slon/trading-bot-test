import asyncio
import configparser
import logging
import sys
import threading
from typing import Dict, Iterable, List

from dotenv import load_dotenv
from sqlalchemy import and_, or_
from typing_extensions import Annotated

import models
import sqlalchemy_enums
from services.price_feed import PriceFeed
from core.alert_server import logger
from core.websockets_updates import BingxWebsocket, symb_list as PRICE_SYM_LIST
from market_api.bingx_client import BingXClient
from core.trading_logic import TradingBot
from models import (
    Api_Key,
    get_all_active_slots,
    get_api_by_id,
    get_apis,
    get_averaging_orders,
    get_orders_by_conditions,
    get_orders_by_key,
    get_settings_by_symbol,
    get_tp_order,
    close_slot
)
from sqlalchemy_enums import OrderTypeEnum

# ────────────────────── init ──────────────────────
load_dotenv()

# шаблоны TradingBot-ов по рынкам
cls_dict: Annotated[
    Dict[sqlalchemy_enums.MarketEnum, TradingBot],
    "Basic TB templates by markets",
] = {}

# ────────────────────── helpers ──────────────────────
def run_async_parallel(coro, args=()):
    asyncio.get_running_loop().create_task(coro(*args))

def websocket_loop(pos_list: List[str]) -> None:
    """
    Отдельный WS-поток для отслеживания тикеров,
    необходимых сравнению с ценами ордеров.
    """
    BingxWebsocket(pos_list, lambda s, p: asyncio.create_task(callback(s, p, 0.0))).start()


# ────────────────────── callbacks / GC ──────────────────────
async def callback(symbol: str, price: float, ts: float) -> None:
    """
    Проверяем лимит-ордера (TP / Averaging) на исполнение по тикеру.
    """
    orders = await get_orders_by_conditions(
        [
            models.Order.position == symbol,
            models.Order.note != sqlalchemy_enums.NoteEnum.BUY,
            models.Order.price != 0,
            models.Order.active == True,  # noqa: E712
            or_(
                and_(
                    models.Order.note == sqlalchemy_enums.NoteEnum.AVERAGING,
                    models.Order.price >= price,
                ),
                and_(
                    models.Order.note == sqlalchemy_enums.NoteEnum.TAKE_PROFIT,
                    models.Order.price == price,
                ),
            ),
        ]
    )

    for order in orders:
        api = await get_api_by_id(order.api_id)
        template_bot = cls_dict[api.market]

        new_bot = TradingBot(
            template_bot.client,
            template_bot.symbol_list,
            template_bot.take_profit_percent,
            template_bot.initial_entry_percent,
            template_bot.averaging_levels,
            template_bot.averaging_percents,
            template_bot.averaging_volume_percents,
            template_bot.use_balance_percent,
            template_bot.max_symbols,
        )
        await new_bot.client.set_credentials(api.api_key, api.secret_key)
        #new_bot.client.api_key = api.api_key
        #new_bot.client.secret_key = api.secret_key
        await new_bot.set_api()

        new_bot.logger.info(
            "Rechecking %s due to %s (tick=%s, order.price=%s)",
            symbol,
            order.note,
            price,
            order.price,
        )
        new_bot._emit(
            "bot_log",
            {
                "msg": f"Rechecking {symbol} due to {order.note} "
                f"(tick={price}, order.price={order.price})"
            },
        )

        await new_bot._process_open_orders(symbol)
        await new_bot._report_status(symbol)


async def check_tp(api: Api_Key, symbol: str):
    template_bot = cls_dict[api.market]
    new_bot = TradingBot(
        template_bot.client,
        template_bot.symbol_list,
        template_bot.take_profit_percent,
        template_bot.initial_entry_percent,
        template_bot.averaging_levels,
        template_bot.averaging_percents,
        template_bot.averaging_volume_percents,
        template_bot.use_balance_percent,
        template_bot.max_symbols,
    )
    await new_bot.client.set_credentials(api.api_key, api.secret_key)
    #new_bot.client.api_key = api.api_key
    #new_bot.client.secret_key = api.secret_key
    await new_bot.set_api()

    await new_bot._process_open_orders(symbol)
    await new_bot._report_status(symbol)


async def garbage_collector(_: object = None):
    """
    Каждые 5 минут:
      • проверяет активные слоты,
      • отменяет «зависшие» averaging-ордера без TP,
      • пересчитывает TP-исполнения.
    """
    while True:
        logger.info("Starting garbage collector...")
        slots = await get_all_active_slots()
        logger.info("Active slots: %s", slots)
        for slot in slots:
            tp_order = await get_tp_order(slot.api_id, slot.position)
            #logger.info(f"[garbage_collector] {slot.position} TP: {tp_order.order_id}")
            orders_avg = await get_averaging_orders(slot.api_id, slot.position)
            api = await get_api_by_id(slot.api_id)
            template_bot = cls_dict[api.market]
            new_bot = TradingBot(
                template_bot.client,
                template_bot.symbol_list,
                template_bot.take_profit_percent,
                template_bot.initial_entry_percent,
                template_bot.averaging_levels,
                template_bot.averaging_percents,
                template_bot.averaging_volume_percents,
                template_bot.use_balance_percent,
                template_bot.max_symbols,
            )
            await new_bot.client.set_credentials(api.api_key, api.secret_key)
            #new_bot.client.api_key = api.api_key
            #new_bot.client.secret_key = api.secret_key
            await new_bot.set_api()
            if not tp_order:
                for avg in orders_avg:
                    await new_bot.client.cancel_order(slot.position, avg.order_id)
                    await asyncio.sleep(1)
                await close_slot(api.pub_id, slot.position)
            else:
                await new_bot.check_tp_filled(slot.position)
        await asyncio.sleep(300)


async def open_callback(
    symbol: str,
    market: sqlalchemy_enums.MarketEnum,
    reason: str,
    action: str = "LONG",
):
    """
    Вызывается Alert-сервером при входящем HTTP-сигнале.
    """
    try:
        logger.info("[open_callback] %s, %s", symbol, reason)

        if symbol in PRICE_SYM_LIST:
            settings = await get_settings_by_symbol(symbol, reason)
            for sett in settings:
                api = await get_api_by_id(sett.api_id)
                if api.market != market:
                    continue

                template_bot = cls_dict[market]
                bot = TradingBot(
                    template_bot.client,
                    template_bot.symbol_list,
                    sett.take_profit_percent,
                    sett.initial_entry_percent,
                    sett.averaging_levels,
                    [float(x) for x in sett.averaging_percents.split(",")],
                    [float(x) for x in sett.averaging_volume_percents.split(",")],
                    sett.use_balance_percent,
                    template_bot.max_symbols,
                )
                await bot.client.set_credentials(api.api_key, api.secret_key)
                #bot.client.api_key = api.api_key
                #bot.client.secret_key = api.secret_key
                await bot.set_api()

                if action.upper() == "LONG":
                    run_async_parallel(bot.open_position, (symbol, reason))
                elif action.upper() == "CLOSE":
                    run_async_parallel(bot.close_position, (symbol,))
    except Exception as e:
        logger.error("Exception in open_callback: %s", e, exc_info=True)


# ────────────────────── BotGUI (PyQt / FastAPI) ──────────────────────
class BotGUI:
    """
    Демонстрационная GUI-обёртка.
    Чтобы оставить этот файл headless, UI-виджеты закомментированы;
    вместо GUI-кнопок используется автозапуск бота (start_bot()).
    """

    def __init__(self, config_path: str = "config.ini"):
        self.config_path = config_path
        self.config = configparser.ConfigParser()
        self.config.read(self.config_path)

        self.bot_instance = None

    # ────────────────────────── config I/O ──────────────────────────
    def _save_config(self):
        # сохранение полей UI → config.ini  (упрощено)
        with open(self.config_path, "w", encoding="utf-8") as f:
            self.config.write(f)
        logger.info("Config saved.")

    # ────────────────────────── MAIN ACTIONS ─────────────────────────
    def start_bot(self):
        """
        Читает конфиг, создаёт BingXClient + шаблон TradingBot,
        запускает garbage-collector и WebSocket-поток.
        """
        logfile = self.config.get("LOGGING", "logfile", fallback="logs/bot.log")
        loglevel = self.config.get("LOGGING", "loglevel", fallback="INFO").upper()

        logging.basicConfig(
            level=getattr(logging, loglevel, logging.INFO),
            format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            handlers=[
                logging.FileHandler(logfile, mode="a", encoding="utf-8"),
                logging.StreamHandler(sys.stdout),
            ],
        )

        api_key = self.config["API"]["KEY"]
        secret_key = self.config["API"]["SECRET"]

        bot_sec = self.config["BOT"]
        symbol = bot_sec.get("SYMBOLS", "SOL-USDT,ADA-USDT")
        max_symbols = bot_sec.getint("MAX_SYMBOLS", 3)
        init_p = bot_sec.getfloat("INITIAL_ENTRY_PERCENT", 10.0)
        tp_p = bot_sec.getfloat("TAKE_PROFIT_PERCENT", 0.7)
        use_bal = bot_sec.getfloat("USE_BALANCE_PERCENT", 50.0)
        lvl = bot_sec.getint("AVERAGING_LEVELS", 3)
        avp_list = [float(x) for x in bot_sec.get("AVERAGING_PERCENTS", "2,4,6").split(",")]
        avv_list = [float(x) for x in bot_sec.get("AVERAGING_VOLUME_PERCENTS", "10,15,20").split(",")]

        symb_list_local = symbol.split(",")

        bingx_client = BingXClient.instance(api_key, secret_key)
        cls_dict[sqlalchemy_enums.MarketEnum.BINGX] = TradingBot(
            bingx_client,
            symb_list_local,
            tp_p,
            init_p,
            lvl,
            avp_list,
            avv_list,
            use_balance_percent=use_bal,
            max_symbols=max_symbols,
        )

        # запускаем GC и WS-подписку
        run_async_parallel(garbage_collector, (None,))
        PriceFeed.instance().subscribe(symb_list_local, callback)

    # ───────────────────────── WS-only режим ─────────────────────────
    def start_ws(self):

        bot_sec = self.config["BOT"]
        symbols = bot_sec.get("SYMBOLS", "SOL-USDT,ADA-USDT").split(",")

        # запускаем поток цен
        websocket_loop(symbols)

    def stop_bot(self):
        if self.bot_instance:
            self.bot_instance.stop()
        logger.info("Bot stopped.")


# ────────────────────── entry-point ──────────────────────
def run_app():
    """
    Headless-режим: читает config.ini и сразу запускает BotGUI.start_bot().
    Для PyQt-версии создайте QApplication, позовите initUI() и show().
    """
    gui = BotGUI()
    gui.start_bot()


if __name__ == "__main__":
    run_app()
