from pydantic import BaseModel, EmailStr, Field
from .trade import SettingsModel, StageConfigModel
import sqlalchemy_enums
from typing import List

class UserRegister(BaseModel):
    email: EmailStr
    password: str
    login: str

class UserGet(UserRegister):
    public_id: str

class TokenModel(BaseModel):
    access_token: str
    refresh_token: str


class UpdateApis(BaseModel):
    market: sqlalchemy_enums.MarketEnum = Field(default=None)
    api_key: str = None
    secret_key: str = None


class ApiKeys(UpdateApis):
    id: int
    pub_id: str

class ApiKey(ApiKeys):
    settings: List[SettingsModel]
    stage_config: List[StageConfigModel]
