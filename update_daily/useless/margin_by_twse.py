import pandas as pd
import os
import datetime


def update_margin_twse(date):
    link = 'https://openapi.twse.com.tw/v1/exchangeReport/MI_MARGN'
    new_margin = pd.read_json(link)
    new_margin.to_csv('融資.csv', encoding='utf-8-sig')
    new_margin = new_margin.rename(columns={'股票代號': 'stock_id', '股票名稱': 'stock_name', '融資買進': 'SBLShortSalesShortCovering',	'融資賣出': 'SBLShortSalesShortSales',	'融資現金償還': 'SBLShortSalesReturns',	'融資前日餘額': 'SBLShortSalesPreviousDayBalance',	'融資今日餘額': 'SBLShortSalesCurrentDayBalance',	'融資限額': 'SBLShortSalesQuota',
                                            '融券買進': 'MarginShortSalesShortCovering',	'融券賣出': 'MarginShortSalesShortSales',	'融券現券償還': 'MarginShortSalesStockRedemption',	'融券前日餘額': 'MarginShortSalesPreviousDayBalance',	'融券今日餘額': 'MarginShortSalesCurrentDayBalance',	'融券限額': 'MarginShortSalesQuota'})
    stock_list = pd.read_csv(os.getcwd()+'/stock_filename_list.csv', encoding='utf-8-sig', dtype={'stock_id': str})
    for index, file_name in stock_list.iterrows():
        stock_no, stock_name = file_name[0].split('_')
        print(file_name[0])
        keptdata = new_margin.loc[new_margin['stock_id'] == stock_no]
        if keptdata.empty:
            print('new data empty, pass')
            continue
        keptdata = keptdata.assign(date=date.date())
        # 將各股今日交易結果聯集到歷史資料
        history_data = pd.read_csv(os.getcwd()+'/test/'+file_name[0], thousands=',', dtype={'stock_id': str})
        history_data = history_data.astype({'SBLShortSalesShortCovering': 'str',	'SBLShortSalesShortSales': 'str',	'SBLShortSalesReturns': 'str', 'SBLShortSalesPreviousDayBalance': 'str',	 'SBLShortSalesCurrentDayBalance': 'str',	 'SBLShortSalesQuota': 'str',
                                            'MarginShortSalesShortCovering': 'str',	'MarginShortSalesShortSales': 'str',	'MarginShortSalesStockRedemption': 'str',	'MarginShortSalesPreviousDayBalance': 'str',	 'MarginShortSalesCurrentDayBalance': 'str', 'MarginShortSalesQuota': 'str'})

        # 如果資料重複,continue
        if history_data.at[history_data.index[-1], 'MarginShortSalesPreviousDayBalance'] == keptdata.loc[keptdata.index[-1], 'MarginShortSalesPreviousDayBalance'] and history_data.at[history_data.index[-1], 'SBLShortSalesPreviousDayBalance'] == keptdata.loc[keptdata.index[-1], 'SBLShortSalesPreviousDayBalance']:
            print('repeat')
            continue
        chips_data = pd.concat([history_data, keptdata], ignore_index=True, join='outer')
        # price_data.to_csv(os.getcwd()+para.STAlert+para.price_dir+file_name,
        #                   encoding='utf-8-sig', index=None)
        # print(chips_data)
        chips_data.to_csv(os.getcwd()+'/test/'+file_name[0], encoding='utf-8-sig', index=None)

    return new_margin


if __name__ == "__main__":
    todate = datetime.datetime.now()  # 2022-12-20
    if todate.hour > 9:
        todate_date = todate-datetime.timedelta(days=1)
    update_margin_twse(todate)
