import requests

url = "http://127.0.0.1:5000/alert"
data = {"symbol":"ADA-USDT","action":"LONG"}
resp = requests.post(url, json=data)
print(resp.status_code, resp.text)
