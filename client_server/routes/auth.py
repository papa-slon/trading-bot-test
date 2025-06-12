import datetime
import os
from functools import wraps
from typing import Dict, Annotated
import models
from client_server.schemas.auth import TokenModel, UserGet, UserRegister
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from jose import JWTError, jwt
from dotenv import load_dotenv

load_dotenv()

ACCESS_EXP = float(os.environ.get("ACCESS_EXPIRE"))
REFRESH_EXP = float(os.environ.get("REFRESH_EXPIRE"))
KEY = os.environ.get("KEY")

router = APIRouter(prefix="/auth")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/dev/auth/login")
cryptoctx = CryptContext(schemes="bcrypt")


class AuthUtils:
    @staticmethod
    def encrypt_pass(plain_pass):
        return cryptoctx.encrypt(plain_pass)

    @staticmethod
    def verify(plain_pass, hash_pass):
        return cryptoctx.verify(plain_pass, hash_pass)

    @staticmethod
    def create_token(payload: Dict):
        payload_refresh = payload.copy()
        payload_access = payload.copy()
        payload_access["type"] = "access"
        payload_access["exp"] = datetime.datetime.now() + datetime.timedelta(minutes=ACCESS_EXP)

        payload_refresh["type"] = "refresh"
        payload_refresh["exp"] = datetime.datetime.now() + datetime.timedelta(minutes=REFRESH_EXP)

        access = jwt.encode(payload_access, key=KEY, algorithm="HS256")
        refresh = jwt.encode(payload_refresh, key=KEY, algorithm="HS256")
        return {
            "access_token": access,
            "refresh_token": refresh
        }

    @staticmethod
    async def get_user_by_token(token: Annotated[oauth2_scheme, Depends()]):
        try:
            print(token)
            payload = jwt.decode(token, key=KEY, algorithms=["HS256"])
            guid = payload["public_id"]
            us = await models.get_user(public_id = guid)
            return us
        except JWTError:
            return {"exception": True, "code": 401, "message": "Неверный токен"}

    @staticmethod
    def admin(fn):
        @wraps(fn)
        async def decorator(*args, **kwargs):
            user = kwargs.get("user")
            if user.role == "admin":
                return await fn(*args, **kwargs)
            else:
                raise HTTPException(status_code=403)

        return decorator


user_dep = Annotated[models.User, Depends(AuthUtils.get_user_by_token)]


@router.post("/register", status_code=201)
async def register(user: UserRegister):
    user_dict = user.model_dump()


    user_dict["password"] = AuthUtils.encrypt_pass(user_dict["password"])
    #res = await db.execute(stmt)
    us = await models.get_user(login=user.login, email=user.email)
    if us:
        return {"status_code": 400, "message": "Данное имя пользователя или почта уже существуют"}

    new_user = await models.add_user(**user_dict)

    return UserGet.model_validate(new_user, from_attributes=True)


@router.post("/login", response_model=TokenModel)
async def login(login_form: Annotated[OAuth2PasswordRequestForm, Depends()]):
    login_ = login_form.username
    user = await models.get_user(login=login_)
    if not user:
        raise HTTPException(status_code=403, detail="Неверный логин или пароль")



    if not AuthUtils.verify(login_form.password, user.password):
        raise HTTPException(status_code=403, detail="Неверный логин или пароль")

    payload = {
        "public_id": user.public_id,
    }

    return AuthUtils.create_token(payload)