from FinMind.data import DataLoader
import pandas as pd
import os
import time
import function.file_dir as file_dir
#import function.calculate_investor_cost as calculate_investor_cost
from function.inves_data_trans import investor_data_transformer
import datetime


def build_margin_data(file_list_name, start_date, end_date):

    # 股票清單
    stock_id_list = pd.read_csv(os.getcwd()+'/'+file_list_name, dtype={'stock_id': str}, index_col=False)

    # 逐一去撈，並存為EXCEL
    for index, d in stock_id_list.iterrows():
        stock_id = d['stock_id']
        stock_name = d['stock_name']
        print(stock_id, stock_name)
        try:
            # 融資融券API
            margin_data = DataLoader().taiwan_daily_short_sale_balances(stock_id, start_date, end_date)
            # 三大法人API
            time.sleep(2)
            investor_data = DataLoader().taiwan_stock_institutional_investors(stock_id, start_date, end_date)
            investor_data = investor_data_transformer(investor_data)  # 轉換格式
            time.sleep(2)
            # 存檔
            filename = stock_id+'_'+stock_name + '.csv'
            # print(margin_data, investor_data)
            chips_data = margin_data.merge(investor_data, on=['date', 'stock_id'])
            print ('original chips data finished')
            #chips_data=calculate_investor_cost.calculate_cost(chips_data)
            chips_data.to_csv(os.getcwd()+file_dir.chip_dir + filename,
                              encoding='utf-8-sig', index=None)
            print(stock_id, stock_name, 'chips_data merge finished')

        except Exception as e:
            print('error happen,reason:', e)
            continue


if __name__ == '__main__':

    target_stocks_list_name = 'tracing_stock_list.csv'
    start_date = '2015-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    build_margin_data(target_stocks_list_name, start_date, end_date)
