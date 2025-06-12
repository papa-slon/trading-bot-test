import logging

from fastapi import FastAPI

from starlette.requests import Request
from starlette.responses import JSONResponse
from models import get_apis
import uvicorn

app = FastAPI(root_path="/dev")

logger = logging.getLogger(__name__)


"""
@app.post("/alert")
async def tv_alert(request: Request):
    
    try:
        data = await request.json()
        logger.info(f"[tv_alert] Received data: {data}")

        if not data:
            return JSONResponse({"status": "error", "msg": "No JSON payload"}, status_code=400)

        symbol = data.get("symbol","")
        action = data.get("action","")

        apis = await get_apis()
        for api in apis:
            market = api.market
            key = api.api_key
            secret = api.secret_key

            bot = cls_dict[market]
            bot.client.api_key = key
            bot.client.secret_key = secret

            if action.upper() == "LONG":
                await bot.open_position(symbol)
                return {"status": "ok", "msg": "open_position called"}
            elif action.upper() == "CLOSE":
                await bot.close_position(symbol)
                return {"status": "ok", "msg": "close_position called"}
            else:
                return JSONResponse({"status": "error", "msg": f"Unknown action {action}"}, status_code=400)

    except Exception as e:
        logger.error(f"Exception on /alert => {e}", exc_info=True)
        return JSONResponse(content = {"status": "error", "msg": str(e)}, status_code = 500)
"""

def start_alert_server( host: str="0.0.0.0", port: int=5000):
    logger.info(f"[start_alert_server] Starting server on {host}:{port} with root_path='/dev'...")
    uvicorn.run(app=app, host=host, port=port, root_path="/dev")
