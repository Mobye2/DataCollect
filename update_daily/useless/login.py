import requests
from FinMind.data import DataLoader
import para

api = DataLoader()
api.login_by_token(api_token=para.token)

'''使用次數'''

url = "https://api.web.finmindtrade.com/v2/user_info"
payload = {
    "token": para.token,
}
resp = requests.get(url, params=payload)
print(resp.json()["user_count"])  # 使用次數
print(resp.json()["api_request_limit"])  # api 使用上限
