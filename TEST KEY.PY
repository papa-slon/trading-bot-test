import time
import requests
import hmac
from hashlib import sha256

APIURL = "https://open-api.bingx.com"
APIKEY = "DsPvP6p8sFdBhmcAmdXsj4pNWD2LYDYZrA3zKE2OgmOnpiJpH6oYMn4l1RXS4WfLPzMhaS4Xp65jvZjQVMFjCg"
SECRETKEY = "QSloQTDDwP3As8JuhJMBWcIHI2UZDmY2qH48sRzwwy882nZ1syV6z4AjQzZJrmecHqQTU5cIu2C6ESHTkmQ"


def demo():
    # FIX: не кладём заранее "timestamp" в paramsMap,
    #      он будет добавлен внутри parseParam автоматически.
    paramsMap = {}

    path = "/openApi/swap/v2/user/balance"  # или /openApi/swap/v2/...
    method = "GET"

    # Генерируем query string
    paramsStr = parseParam(paramsMap)  # вернёт что-то вроде "timestamp=XYZ"
    return send_request(method, path, paramsStr)


def parseParam(paramsMap):
    """
    Сортируем параметры, склеиваем в строку вида "key1=val1&key2=val2".
    Если пусто, добавляем timestamp=...
    Если не пусто, добавляем &timestamp=...
    """
    # Сортируем ключи, формируем базовую строку
    sortedKeys = sorted(paramsMap.keys())
    base_str = "&".join([f"{k}={paramsMap[k]}" for k in sortedKeys])

    # Генерируем fresh timestamp (мс)
    now_ts = str(int(time.time() * 1000))

    if base_str:
        # Если уже есть параметры, добавим &timestamp=...
        return base_str + "&timestamp=" + now_ts
    else:
        # Иначе вернём timestamp=...
        return "timestamp=" + now_ts


def send_request(method, path, urlpa):
    """
    Принимает method (GET/POST), путь (/openApi/...),
    и заранее сформированную строку параметров (urlpa).
    """
    # 1) Считаем подпись
    #    Обычно BingX требует параметр "sign=",
    #    но вы пишете "signature=" => возможно надо заменить.
    #    Допустим, всё же нужен &sign=...
    signature = get_sign(SECRETKEY, urlpa)

    # 2) Полный URL вида:
    #    https://open-api.bingx.com/openApi/swap/v3/user/balance?timestamp=...&sign=...
    # FIX: меняем "signature=" -> "sign=", если того требует документация.
    url = f"{APIURL}{path}?{urlpa}&sign={signature}"

    print(">>> Final GET URL:", url)

    headers = {
        "X-BX-APIKEY": APIKEY,
        # FIX: BingX часто требует этот заголовок при GET/POST
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Поскольку GET, data не нужно
    response = requests.request(method, url, headers=headers)
    return response.text


def get_sign(api_secret, payload):
    """
    Подпись HMAC SHA256(payload) с секретом.
    """
    signature = hmac.new(
        api_secret.encode("utf-8"),
        payload.encode("utf-8"),
        digestmod=sha256
    ).hexdigest()

    print("sign =", signature)
    return signature


if __name__ == '__main__':
    print("demo:", demo())

