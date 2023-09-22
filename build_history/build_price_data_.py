from talib import abstract
from FinMind.data import DataLoader
import pandas as pd
import os
import datetime
import function_pack.file_dir as file_dir
from function_pack.api_limit_error import api_upper_limit_error
from function_pack.build_list import build_list

def price_indicators(df: pd.DataFrame):
    df = df.rename(columns={'max': 'high', 'min': 'low'})
    # df = df.rename(columns={'open': 'Open', 'close': 'Close', 'volume': 'Volume','Trading_volume': 'Volume'})
    # KD
    df = pd.concat([df, abstract.STOCH(df, fastk_period=9, slowk_period=3, slowd_period=3)], axis=1)
    # MA
    df = pd.concat([df, abstract.SMA(df, 5)], axis=1)
    df = df.rename(columns={0: 'five_MA'})
    df = pd.concat([df, abstract.SMA(df, 10)], axis=1)
    df = df.rename(columns={0: 'ten_MA'})
    df = pd.concat([df, abstract.SMA(df, 20)], axis=1)
    df = df.rename(columns={0: 'twenty_MA'})
    # RSI
    df = pd.concat([df, abstract.RSI(df, 14)], axis=1)
    df = df.rename(columns={0: 'RSI'})
    # MACD macd, macdsignal, macdhist，分別就是 快線、慢線、柱狀圖
    df = pd.concat([df, abstract.MACD(df, fastperiod=12, slowperiod=26, signalperiod=9)], axis=1)
    df = df.rename(columns={0: 'MACD'})
    # 布林通道
    df = pd.concat([df, abstract.BBANDS(df, timeperiod=20, nbdevup=2.0, nbdevdn=2.0, matype=0)], axis=1)
    df = df.rename(columns={0: 'BBANDS'})
    df = df.loc[:, ~df.columns.duplicated(keep='last')]
    # print(df)
    df = df.rename(columns={'high': 'High', 'low': 'Low'})
    df = df.rename(columns={'open': 'Open', 'close': 'Close', 'volume': 'Volume','Trading_volume': 'Volume'})


    return df

@api_upper_limit_error
def call_price_data(stock_id, start_date, end_date):
    #用FINMIND API去CALL資料
    data = DataLoader().taiwan_stock_daily(stock_id, start_date, end_date)
    return data

def build_price_data(target_stocks_list_name, start_date, end_date):

    # 追蹤股票清單, 檔名拆為id & name
    stock_id_list = pd.read_csv(os.getcwd()+'/'+target_stocks_list_name, index_col=False)
    stock_id_list[['stock_id','stock_name']] = stock_id_list['file_name'].str.split('_',expand=True)
    stock_id_list[['stock_name','extension']] = stock_id_list['stock_name'].str.split('.',expand=True)

    # 依清單逐一去撈，並存為EXCEL
    for index, filename in stock_id_list.iterrows():
        stock_id = filename['stock_id']
        stock_name = filename['stock_name']
        print(stock_id, stock_name)
        #用FINMIND API去CALL資料
        data = call_price_data(stock_id, start_date, end_date)
        if data.empty == True:
            continue
        #增加5MA之類的指標
        data = price_indicators(data)
        #存檔
        data.to_csv(os.getcwd()+file_dir.price_dir+filename['file_name'],
                    encoding='utf-8-sig', index=False)
        print('build_price_data finished')


if __name__ == '__main__':

    #target_stocks_list_name = 'tracing_stock_list.csv'
    stock_filemame_list = 'stock_filename_list.csv'
    start_date = '2015-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    build_price_data(stock_filemame_list, start_date, end_date)

    # 建立以檔名列表
    #build_list(file_dir.price_dir, stock_filemame_list)
