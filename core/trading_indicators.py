from __future__ import annotations

import asyncio
import configparser
import json
import logging
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import aiohttp
import pandas as pd
import pandas_ta as ta
import websockets

from core.trading_logic import TradingBot
from market_api.binance_client import BinanceClient
from models import (
    get_timeframes_lens,
    get_signals_by_df,
    get_api_by_id,
    get_settings_by_symbol, get_timeframes_smas, get_shorts,
)
from pandas import DatetimeIndex
from sqlalchemy_enums import MarketEnum, ReasonEnum
from utils.symbols import to_binance
from market_api.bingx_client import BingXClient, DEFAULT_RPS

# ───────── GUI (lazy import) ─────────
def _gui_refs():
    from gui import app_gui as _ag
    return _ag.cls_dict, _ag.logger


# ────────── конфигурация ──────────
_cfg = configparser.ConfigParser()
_cfg.read("config.ini")

symb_list: List[str] = _cfg["BOT"]["SYMBOLS"].split(",")
bin2orig: Dict[str, str] = {to_binance(s): s for s in symb_list}

# ────────── хранилище данных ──────────
COLS = ["time", "open", "high", "low", "close", "volume"]
main_dict: Dict[str, pd.DataFrame] = {s: pd.DataFrame(columns=COLS) for s in symb_list}
locks: Dict[str, asyncio.Lock] = {s: asyncio.Lock() for s in symb_list}

latest_open_ms: Dict[str, int] = {s: -1 for s in symb_list}
done_bars: Dict[Tuple[str, str], pd.Timestamp] = {}
l_f = _cfg["LOGGING"]["LOGFILE"]

# ────────── логирование ──────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(l_f, mode="a", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)
logger = logging.getLogger(__name__)

# ────────── helper-функции ──────────
def _trim(df: pd.DataFrame, max_rows: int = 7000) -> pd.DataFrame:
    return df.tail(max_rows).reset_index(drop=True) if len(df) > max_rows else df


def _add_indicators(
    df: pd.DataFrame, *, rsi: int | None = None, cci: int | None = None
) -> pd.DataFrame:
    df_sorted = df.sort_values("time").reset_index(drop=True)
    out = df_sorted.loc[:, ["open", "high", "low", "close", "volume"]].copy()
    if rsi:
        out["rsi"] = ta.rsi(out["close"], length=rsi)
    if cci:
        out["cci"] = ta.cci(out["high"], out["low"], out["close"], length=cci)
    out["delta"] = (out["close"] - out["open"]) * out["volume"]
    return out

def sma_(df: pd.DataFrame, slow: int, fast: int) -> pd.DataFrame:
    out = df.copy()
    out["slow"] = ta.sma(close = out["close"], length=slow)
    out["fast"] = ta.sma(out["close"], length=fast)
    #print(out)
    return out


# ────────── перевод tf → минуты ──────────
def _tf_to_minutes(tf: str) -> int:
    num, unit = int(tf[:-1]), tf[-1].lower()
    if unit == "m":
        return num
    if unit == "h":
        return num * 60
    if unit == "d":
        return num * 1_440
    raise ValueError(f"Unsupported timeframe: {tf}")


# ────────── расчёт индикаторов и генерация сигналов ──────────
async def _calc_and_signal(symbol: str) -> None:
    df = main_dict[symbol]
    if df.empty:
        return
    t_n = int(time.time())//60
    now = datetime.utcnow().replace(second=0, microsecond=0)
    df = df.sort_values("time").reset_index(drop=True)

    for tf, rsi_lens, cci_lens in await get_timeframes_lens():
        minutes = _tf_to_minutes(tf)

        tmp = df.assign(idx=pd.to_datetime(df["time"], unit="ms")).set_index("idx")
        bars = (
            tmp.resample(f"{minutes}min", closed="left", label="left", origin="epoch")
               .agg(open=("open","first"), high=("high","max"),
                    low=("low","min"), close=("close","last"),
                    volume=("volume","sum"))
               .dropna()
        )


        if bars.empty:
            continue

        last_open  = bars.index[-1]
        last_close = last_open + timedelta(minutes=minutes)

        if now < last_close:
            bars = bars[:-1]

        if bars.empty:
            continue
        bars = (bars.reset_index()
                     .rename(columns={"idx":"time"}))
        bars["time"] = bars["time"].astype("int64") // 10**6

        for rlen in rsi_lens:
            rsi_df = _add_indicators(bars, rsi=rlen)
            for api_id, reason in await get_signals_by_df(
                    df=rsi_df, symbol=symbol, rsi_len=rlen, timeframe=tf):
                await _dispatch(api_id, symbol, reason)

        for clen in cci_lens:
            cci_df = _add_indicators(bars, cci=clen)
            for api_id, reason in await get_signals_by_df(
                    df=cci_df, symbol=symbol, cci_len=clen, timeframe=tf):
                await _dispatch(api_id, symbol, reason)

        logger.debug(
            "[%s %s] tf=%s  prevRSI=%.2f  curRSI=%.2f  threshold=%s",
            symbol,
            bars['time'].iloc[-1],
            tf,
            prev.rsi if 'prev' in locals() else float('nan'),
            cur.rsi if 'cur' in locals() else float('nan'),
            '—'  # порог узнаём в get_signals_by_df
        )


# ────────── отправка сигнала в торгового бота ──────────

async def _dispatch(api_id: str, symbol: str, reason) -> None:
    """
    Создаём *отдельный* BingX-client под нужный API-ключ и
    передаём его в TradingBot. Благодаря этому ключи/секреты
    разных аккаунтов больше не «перетирают» друг друга.
    """
    api  = await get_api_by_id(api_id)
    sett = await get_settings_by_symbol(symbol, reason, api_id)
    cls_dict, log = _gui_refs()

    log.info(f"Signal({reason}) {symbol} via {api_id}")

    # ── 1. создаём НОВЫЙ клиент именно под этот ключ ──────────────────
    new_client = BingXClient(api.api_key, api.secret_key, rps=DEFAULT_RPS)

    # ── 2. формируем TradingBot на базе шаблона, но с новым client ────
    template_bot = cls_dict[MarketEnum.BINGX]

    bot = TradingBot(
        new_client,                              # ← свой объект клиента
        template_bot.symbol_list,
        sett.take_profit_percent,
        sett.initial_entry_percent,
        sett.averaging_levels,
        [float(x) for x in sett.averaging_percents.split(",")],
        [float(x) for x in sett.averaging_volume_percents.split(",")],
        sett.use_balance_percent,
        sett.leverage,
        template_bot.max_symbols,
        sett.trailing_stop_percentage,
        sett.activation_price,
        sett.base_stop,
    )

    # TradingBot сам сохранит ссылку на api-запись
    await bot.set_api()
    await bot.open_position(symbol, reason)



# ────────── загрузка истории и запуск Binance-WS ──────────
async def _load_history() -> None:
    _, log = _gui_refs()
    rest = BinanceClient()
    log.info("[BINANCE] loading history.")
    for s in symb_list:
        try:
            kl = await rest.get_klines(to_binance(s), limit=6000)
            kl = kl[:-1]
            main_dict[s] = pd.DataFrame(
                [(int(x[0]), *map(float, x[1:6])) for x in kl], columns=COLS
            )
            latest_open_ms[s] = main_dict[s]["time"].iloc[-1]
        except aiohttp.ClientResponseError as e:
            log.warning(f"{s} skip history ({e.status})")
        except:
            print(kl, s)


async def _start_ws() -> None:
    streams = "/".join(f"{to_binance(s).lower()}@kline_1m" for s in symb_list)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    while True:
        try:
            asyncio.get_running_loop()
            async with websockets.connect(url, ping_interval=15) as ws:
                async for raw in ws:
                    await _on_kline(json.loads(raw))
        except Exception as exc:
            logger.error(f"WS error: {exc}, reconnecting in 5s")
            await asyncio.sleep(5)


# ────────── публичный API модуля ──────────
async def startup() -> None:
    await _load_history()
    asyncio.create_task(_start_ws())


async def main_loop() -> None:
    while True:
        await asyncio.sleep(3600)

#NEW 05.05

async def _dispatch_short(api_id: str, symbol: str):
    api = await get_api_by_id(api_id)
    sett = await get_settings_by_symbol(symbol, None, api_id)
    cls_dict, log = _gui_refs()

    log.info(f"Signal(SHORT) {symbol} via {api_id}")

    # ── 1. создаём НОВЫЙ клиент именно под этот ключ ──────────────────
    new_client = BingXClient(api.api_key, api.secret_key, rps=DEFAULT_RPS)

    # ── 2. формируем TradingBot на базе шаблона, но с новым client ────
    template_bot = cls_dict[MarketEnum.BINGX]

    bot = TradingBot(
        new_client,  # ← свой объект клиента
        template_bot.symbol_list,
        sett.take_profit_percent,
        sett.initial_entry_percent,
        sett.averaging_levels,
        [float(x) for x in sett.averaging_percents.split(",")],
        [float(x) for x in sett.averaging_volume_percents.split(",")],
        sett.use_balance_percent,
        sett.leverage,
        template_bot.max_symbols,
        sett.trailing_stop_percentage,
        sett.activation_price,
        sett.base_stop
    )

    # TradingBot сам сохранит ссылку на api-запись
    await bot.set_api()
    await bot.open_position(symbol, ReasonEnum.YELLOW)

async def calc_short(symbol: str):
    df = main_dict[symbol].copy()
    if df.empty:
        return
    t_n = int(time.time()) // 60
    now = datetime.utcnow().replace(second=0, microsecond=0)
    df = df.sort_values("time").reset_index(drop=True)
    tfs = await get_timeframes_smas()
    #print(tfs)
    #return
    for tf, lens in tfs:
        minutes = _tf_to_minutes(tf)

        #tmp = df.assign(idx=pd.to_datetime(df["time"], unit="ms")).set_index("idx")
        df["idx"] = pd.to_datetime(df["time"], unit="ms")
        df.set_index(DatetimeIndex(df["idx"]), inplace=True)
        bars = (
            df.resample(f"{minutes}min", closed="left", label="left", origin="epoch")
            .agg(open=("open", "first"), high=("high", "max"),
                 low=("low", "min"), close=("close", "last"),
                 volume=("volume", "sum"))
            .dropna()
        )
        if bars.empty:
            continue

        last_open = bars.index[-1]
        last_close = last_open + timedelta(minutes=minutes)

        if now < last_close:
            bars = bars[:-1]

        if bars.empty:
            continue
        bars = (bars.reset_index()
                .rename(columns={"idx": "time"}))
        bars["time"] = bars["time"].astype("int64") // 10 ** 6
        for fast, slow in lens:
            sma_df = sma_(bars, slow, fast)
            prev, last = sma_df.iloc[-2], sma_df.iloc[-1]
            if prev["fast"]>prev["slow"] and last["fast"]<last["slow"]:
                api_ids = await get_shorts(symbol, tf, fast, slow)
                for api_id in api_ids:
                    await _dispatch_short(api_id, symboll)

# ────────── WebSocket-callback ──────────
async def _on_kline(msg: dict) -> None:
    try:
        raw_symb, _ = msg.get("stream", "").split("@")
        symb = bin2orig.get(raw_symb.upper())
        k = msg.get("data", {}).get("k", {})

        required_keys = {"t", "o", "h", "l", "c", "v", "x"}
        if symb is None or not required_keys.issubset(k) or not k["x"]:
            return

        cur_open_ms = int(k["t"])
        if cur_open_ms == latest_open_ms[symb]:
            df = main_dict[symb]
            df.loc[df.index[-1], ["open", "high", "low", "close", "volume"]] = (
                float(k["o"]),
                float(k["h"]),
                float(k["l"]),
                float(k["c"]),
                float(k["v"]),
            )
            await calc_short(symb)
        else:
            latest_open_ms[symb] = cur_open_ms
            df = main_dict[symb]
            df.loc[len(df)] = (
                cur_open_ms,
                float(k["o"]),
                float(k["h"]),
                float(k["l"]),
                float(k["c"]),
                float(k["v"]),
            )
            main_dict[symb] = _trim(df)
            await calc_short(symb)
    except Exception as e:
        logger.error(f"_on_kline: {e}", exc_info=True)


# ────────── точка входа для отладки ──────────
if __name__ == "__main__":
    asyncio.run(startup())
