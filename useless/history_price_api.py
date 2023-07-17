import requests
import pandas as pd

def tsf():
    print ('tsf is called')

def history_price_api(stock_id, start_date, end_date):
    # 請將 YOUR_API_KEY 替換為您的FinMind API金鑰
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyMy0wMy0yMCAxNTozNTowMCIsInVzZXJfaWQiOiJtb3V0aGJvbWIiLCJpcCI6IjIyMC4xMzIuODYuODEifQ.uFyOi1tCHEkYae4iH3WrO8xkkgEJcpSXzcJQ9Z8Ez6s'
    }

    # 呼叫API並下載資料
    url = f'https://api.finmindtrade.com/api/v4/data'
    params = {
        "dataset": "TaiwanStockPrice",
        "data_id": stock_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    res = requests.get(url, params=params, headers=headers)
    data = pd.DataFrame(res.json()['data'])

    # 處理資料格式
    data['date'] = pd.to_datetime(data['date'])
    data.set_index('date', inplace=True)
    data.sort_index(inplace=True)

    # 輸出結果
    # print(data)
    return data


if __name__ == '__main__':
    # 設定要下載的資料範圍
    stock_id = '2330'
    start_date = '2023-01-01'
    end_date = '2023-03-20'
    print(history_price_api(stock_id, start_date, end_date))
    print('git')
