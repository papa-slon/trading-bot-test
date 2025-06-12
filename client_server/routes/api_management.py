from typing import List

from fastapi import APIRouter
from starlette.responses import JSONResponse

from client_server.routes.auth import user_dep
from client_server.schemas.auth import UpdateApis, ApiKeys, ApiKey
from client_server.schemas.trade import SettingsModel, StageConfigModel
from core.alert_server import logger
from models import add_api_key, delete_api, get_user_api_keys, update_api, update_settings, get_api_by_id, update_stage_config

router = APIRouter(prefix="/api_management")


@router.post("/", response_model=ApiKeys)
async def new_api_key(api_key: UpdateApis, user: user_dep):
    try:
        new_api = await add_api_key(api_key.api_key, api_key.secret_key, api_key.market, user.public_id)
        return new_api
    except Exception as e:
        logger.exception(f"Adding API key for user {user.login} failed: {e}", exc_info=True)
        raise

@router.delete("/{api_id}")
async def delete_api_key(api_id: str, user: user_dep):
    await delete_api(api_id, user.public_id)

@router.get("/", response_model=List[ApiKeys])
async def get_api_keys(user: user_dep):
    apis = await get_user_api_keys(user.public_id)
    print(apis)
    ret = [ApiKeys.model_validate(api, from_attributes=True) for api in apis]
    return ret

@router.get("/{api_id}")
async def get_api_key(api_id: str, user: user_dep):
    api = await get_api_by_id(api_id)
    if api.user_id == user.public_id:
        print(api)
        return ApiKey.model_validate(api, from_attributes=True)

@router.patch("/{api_id}", response_model=ApiKey)
async def patch_api_key(api_id: str, api_key: UpdateApis, user: user_dep):
    res = await update_api(api_id, user.public_id, api_key.model_dump())
    return res

@router.post("/{api_id}/stage_config")
async def update_stage_config_func(api_id: str, user: user_dep, stage_cfg: List[StageConfigModel]):
    res = await update_stage_config(api_id, user.public_id, stage_cfg)
    return res

@router.post("/{api_id}/settings", response_model=List[SettingsModel])
async def update_settings_func(api_id: str, settings: List[SettingsModel], user: user_dep):
    for setting in settings:
        avg_vol = setting.averaging_percents
        avg_drop = setting.averaging_percents

        if len(avg_drop.split(",")) == len(avg_vol.split(",")) == setting.averaging_levels:
            continue
        return JSONResponse(status_code = 422, content = {
            "message": "Length of averaging percentage must be equal to max averaging orders.",
            "exception": True
        })

    res = await update_settings(api_id, user.public_id, settings)
    api = await get_api_by_id(api_id)
    res = [SettingsModel.model_validate(setting, from_attributes=True) for setting in api.settings]
    return res