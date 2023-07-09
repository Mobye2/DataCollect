import pandas as pd
import os
import file_dir 
from talib import abstract
import talib
import matplotlib.pyplot as plt


def increase_indicators(df: pd.DataFrame):
    df = df.rename(columns={'max': 'high', 'min': 'low'})
    # KD
    df = pd.concat([df, abstract.STOCH(df, fastk_period=9, slowk_period=3, slowd_period=3)], axis=1)
    # MA
    df = pd.concat([df, abstract.SMA(df, 5)], axis=1)
    df = df.rename(columns={0: '5MA'})
    df = pd.concat([df, abstract.SMA(df, 10)], axis=1)
    df = df.rename(columns={0: '10MA'})
    df = pd.concat([df, abstract.SMA(df, 20)], axis=1)
    df = df.rename(columns={0: '20MA'})
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

    return df


def main():
    df = pd.read_csv(os.getcwd()+file_dir.price_dir+'2330_台積電.csv')
    df = increase_indicators(df)
    df.to_csv(os.getcwd()+file_dir.price_dir+'2330_台積電.csv', encoding='utf-8-sig', index=None)


if __name__ == '__main__':
    main()
