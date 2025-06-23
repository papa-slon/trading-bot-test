import configparser
from threading import Thread
import asyncio
import uvicorn
import atexit
from market_api.bingx_client import BingXClient
from fastapi_offline import FastAPIOffline
from starlette.requests import Request
from starlette.responses import JSONResponse
from copy import deepcopy
from core.alert_server import logger
from core.trading_indicators import startup, main_loop
from core.trading_logic import TradingBot
from gui.app_gui import cls_dict, BotGUI, callback, run_async_parallel
from models import get_apis, get_settings_by_symbol
from client_server.routes import auth, api_management
from sqlalchemy_enums import ReasonEnum

config_path = "config.ini"
config = configparser.ConfigParser()
config.read(config_path)
symbs = config["BOT"]["SYMBOLS"]

symb_list = symbs.split(",")
fast_app = FastAPIOffline(root_path="/dev")

fast_app.include_router(auth.router)
fast_app.include_router(api_management.router)

@fast_app.post("/test_price")
async def test_price(request: Request):
    data = await request.json()
    await callback(data["symbol"], float(data["price"]), 0.0)


#@fast_app.post("/alert")
async def tv_alert(request: Request):
    """
    POST /alert
    JSON: {"symbol":"ADA-USDT","action":"LONG"}
       or {"symbol":"ADA-USDT","action":"CLOSE"}
    """
    try:
        data = await request.json()
        logger.info(f"[tv_alert] Received data: {data}")

        if not data:
            return JSONResponse({"status": "error", "msg": "No JSON payload"}, status_code=400)

        symbol = data.get("symbol","")
        action = data.get("action","")
        reason = data.get("reason","")

        dict_reason = {"CCI": ReasonEnum.RED, "RSI": ReasonEnum.YELLOW}

        if symbol in symb_list:
            print(data)
            apis = await get_apis()
            for api in apis:
                market = api.market
                key = api.api_key
                secret = api.secret_key

                cls = deepcopy(cls_dict[market])
                sett = await get_settings_by_symbol(symbol, dict_reason[reason], api.pub_id)
                bot = TradingBot(
                    cls.client,
                    cls.symbol_list,
                    cls.take_profit_percent,
                    cls.initial_entry_percent,
                    cls.averaging_levels,
                    cls.averaging_percents,
                    cls.averaging_volume_percents,
                    cls.use_balance_percent,
                    cls.max_symbols,
                    sett.trailing_stop_percentage,
                    sett.activation_price,
                    sett.base_stop,
                )
                bot.client.api_key = key
                bot.client.secret_key = secret
                await bot.set_api()
                print(bot.client.api_key, bot.client.secret_key)
                if action.upper() == "LONG":
                    #await bot.open_position(symbol)
                    #return {"status": "ok", "msg": "open_position called"}

                    run_async_parallel(bot.open_position, (symbol, dict_reason[reason]))
                elif action.upper() == "CLOSE":
                    #await bot.close_position(symbol)
                    run_async_parallel(bot.close_position, (symbol, ))
                    #return {"status": "ok", "msg": "close_position called"}
                else:
                    return JSONResponse({"status": "error", "msg": f"Unknown action {action}"}, status_code=400)
        else:
            logger.info("[alert]: symbol not found => skipping")

    except Exception as e:
        logger.error(f"Exception on /alert => {e}", exc_info=True)
        return JSONResponse(content = {"status": "error", "msg": str(e)}, status_code = 500)

async def start_alert_server():
    host = config["SERVER"].get("HOST_ALERT", "0.0.0.0")
    port = config["SERVER"].getint("PORT_ALERT", 5000)

    #uvicorn.run(app=fast_app, host=host, port=port, )

    await BingXClient.reset_singleton()   # корректно освобождаем и сбрасываем
    BotGUI().start_bot()
    await startup()
    #BotGUI().start_ws()
    task = asyncio.create_task(main_loop())
    #Thread(target = asyncio.run(main_loop()), daemon=True).start()
    Thread(target=uvicorn.run, kwargs={
        "app": fast_app,
        "host": host,
        "port": port,
        "root_path": "/dev"
    }, daemon=True).start()

    logger.info(f"[start_alert_server] Starting server on {host}:{port} with root_path='/dev'...")
    await task



if __name__ == "__main__":
    atexit.register(lambda: asyncio.run(BingXClient.instance("", "").aclose()))
    asyncio.run(start_alert_server())
