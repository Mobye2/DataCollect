import requests
import pandas as pd
import numpy as np
import datetime
import os
from function.indicators_test import increase_indicators
import file_dir


def update_price(date):
    # 取得當日OTC資料
    url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw"
    response = requests.get(url)
    df = pd.read_json(response.text, orient='index', encoding='utf8')
    OTC_data = pd.DataFrame(df.at['aaData', 0], columns=['stock_id', 'stock_name', 'close', 'spread', 'open', 'high',
                            'low', 'Trading_Volume', 'Trading_money', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'])
    OTC_data = OTC_data[['stock_id', 'stock_name', 'close', 'spread', 'open', 'high', 'low', 'Trading_Volume', 'Trading_money']]
    OTC_data = OTC_data[OTC_data['stock_id'].str.len() < 6]  # 篩掉莫名ETF與權證等

    # 取得當日上市股票資料
    listed_stock_data = pd.read_csv('https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=open_data', thousands=",")
    listed_stock_data = listed_stock_data.rename(columns={'證券代號': 'stock_id', '證券名稱': 'stock_name', '成交股數': 'Trading_Volume', '成交金額': 'Trading_money',
                                                          '開盤價': 'open', '最高價': 'high', '最低價': 'low', '收盤價': 'close', '漲跌價差': 'spread', '成交筆數': 'transactions'})
    new_data = pd.concat([OTC_data, listed_stock_data], axis=0, join='inner')
    #new_data.to_csv('price_data.csv', encoding='utf-8-sig', index=False)

    stock_list = pd.read_csv(os.getcwd()+'/stock_filename_list.csv', encoding='utf-8-sig', dtype={'stock_id': str})
    for index, file_name in stock_list.iterrows():
        stock_no, stock_name = file_name[0].split('_')
        print(file_name[0])
        keptdata = new_data.loc[new_data['stock_id'] == stock_no]
        if keptdata.empty:
            print('new data empty, pass')
            continue
        keptdata = keptdata.assign(date=date.date())
        # 將各股今日交易結果聯集到歷史資料
        history_data = pd.read_csv(os.getcwd()+file_dir.price_dir+file_name[0], thousands=',', dtype={'stock_id': str})
        # 如果資料重複,continue
        if history_data.at[history_data.index[-1], 'Trading_Volume'] == keptdata.loc[keptdata.index[-1], 'Trading_Volume'] and history_data.at[history_data.index[-1], 'Trading_money'] == keptdata.loc[keptdata.index[-1], 'Trading_money']:
            print('repeat')
            continue
        price_data = pd.concat([history_data, keptdata], ignore_index=True, join='inner')
        price_data = increase_indicators(price_data)
        price_data.to_csv(os.getcwd()+file_dir.price_dir+file_name[0], encoding='utf-8-sig', index=None)


def update_chip_twse(todate):

    # 取得融資資料
    link = 'https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN'
    new_margin = pd.read_json(link)
    new_margin.to_csv('融資.csv', encoding='utf-8-sig')
    new_margin = new_margin.rename(columns={'股票代號': 'stock_id', '股票名稱': 'stock_name', '融資買進': 'SBLShortSalesShortCovering',	'融資賣出': 'SBLShortSalesShortSales',	'融資現金償還': 'SBLShortSalesReturns',	'融資前日餘額': 'SBLShortSalesPreviousDayBalance',	'融資今日餘額': 'SBLShortSalesCurrentDayBalance',	'融資限額': 'SBLShortSalesQuota',
                                            '融券買進': 'MarginShortSalesShortCovering',	'融券賣出': 'MarginShortSalesShortSales',	'融券現券償還': 'MarginShortSalesStockRedemption',	'融券前日餘額': 'MarginShortSalesPreviousDayBalance',	'融券今日餘額': 'MarginShortSalesCurrentDayBalance',	'融券限額': 'MarginShortSalesQuota', '資券互抵': 'MarginvsSBL', '註記': 'note'})
    # 空值改為0
    new_margin.replace('', 0, inplace=True)
    # 調整欄位類型
    cols_to_convert = [col for col in new_margin.columns if col not in ['stock_id', 'stock_name', 'note'] and new_margin[col].dtype == 'object']
    new_margin[cols_to_convert] = new_margin[cols_to_convert].astype(int)
    new_margin[cols_to_convert] = new_margin[cols_to_convert] * 1000

    # 取得法人資料
    url = 'https://www.twse.com.tw/fund/T86'
    data = {
        'response': 'json',
        'selectType': 'ALLBUT0999',
        'date': todate.strftime("%Y%m%d"),
        'sort': 'by_issue'
    }
    response = requests.post(url, data=data)
    df = pd.read_json(response.text, orient='index', encoding='utf8')
    new_invest_data = pd.DataFrame(df.at['data', 0], columns=df.at['fields', 0])
    new_invest_data = new_invest_data.rename(columns={'證券代號': 'stock_id', '證券名稱': 'stock_name', '外陸資買進股數(不含外資自營商)': 'Foreign_Investor_Buy', '外陸資賣出股數(不含外資自營商)': 'Foreign_Investor_Sell', '外陸資買賣超股數(不含外資自營商)': 'Foreign_Investor_Diff', '投信買進股數': 'Investment_Trust_Buy', '投信賣出股數': 'Investment_Trust_Sell',
                                                      '投信買賣超股數': 'Investment_Trust_Diff', '自營商買賣超股數': 'Dealer_Diff', '自營商買進股數(自行買賣)': 'Dealer_Self_Buy', '自營商賣出股數(自行買賣)': 'Dealer_Self_Sell', '自營商買賣超股數(自行買賣)': 'Dealer_Self_Diff', '自營商買進股數(避險)': 'Dealer_Hedging_Buy', '自營商賣出股數(避險)': 'Dealer_Hedging_Sell', '自營商買賣超股數(避險)': 'Dealer_Hedging_Diff', '三大法人買賣超股數': 'Investors_Diff'})
    new_invest_data = new_invest_data.applymap(lambda x: x.replace(',', '') if type(x) is str else x)

    # 依股票名依序處理
    stock_list = pd.read_csv(os.getcwd()+'/stock_filename_list.csv', encoding='utf-8-sig', dtype={'stock_id': str})
    for index, file_name in stock_list.iterrows():
        stock_no, stock_name = file_name[0].split('_')
        print(file_name[0])

        try:
            margin_kept = new_margin.loc[new_margin['stock_id'] == stock_no]
            invest_kept = new_invest_data.loc[new_invest_data['stock_id'] == stock_no]
            if margin_kept.empty == True or invest_kept.empty == True:
                print('new data is empty, pass')
                continue
            margin_kept = margin_kept.assign(date=todate.date())
            invest_kept = invest_kept.assign(date=todate.date())
            invest_kept = invest_kept.drop(['stock_name', '外資自營商買進股數', '外資自營商賣出股數', '外資自營商買賣超股數'], axis=1)
            chips_kept = margin_kept.merge(invest_kept, on=['date', 'stock_id'])
            # 將各股今日交易結果聯集到歷史資料
            history_data = pd.read_csv(os.getcwd()+'/chips_record/'+file_name[0], thousands=',', dtype={'stock_id': str})
            history_data = history_data.astype({'SBLShortSalesShortCovering': 'str',	'SBLShortSalesShortSales': 'str',	'SBLShortSalesReturns': 'str', 'SBLShortSalesPreviousDayBalance': 'str',	 'SBLShortSalesCurrentDayBalance': 'str',	 'SBLShortSalesQuota': 'str',
                                                'MarginShortSalesShortCovering': 'str',	'MarginShortSalesShortSales': 'str',	'MarginShortSalesStockRedemption': 'str',	'MarginShortSalesPreviousDayBalance': 'str',	 'MarginShortSalesCurrentDayBalance': 'str', 'MarginShortSalesQuota': 'str'})
            # 如果資料重複,continue
            if history_data.at[history_data.index[-1], 'MarginShortSalesPreviousDayBalance'] == margin_kept.loc[margin_kept.index[-1], 'MarginShortSalesPreviousDayBalance'] and history_data.at[history_data.index[-1], 'SBLShortSalesPreviousDayBalance'] == margin_kept.loc[margin_kept.index[-1], 'SBLShortSalesPreviousDayBalance']:
                print('repeat')
                continue
            chips_kept = pd.concat([history_data, chips_kept], ignore_index=True, join='outer')
            chips_kept.to_csv(os.getcwd()+file_dir.chip_dir+file_name[0], encoding='utf-8-sig', index=None)
        except Exception as e:
            print(f"发生错误：{e}")
            continue


if __name__ == '__main__':
    todate = datetime.datetime.now()-datetime.timedelta(days=1)  # 2022-12-20
    print(todate.strftime("%Y%m%d"))
    update_price(todate)
    # update_chip_twse(todate)
