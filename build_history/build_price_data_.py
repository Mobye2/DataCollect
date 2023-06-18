from FinMind.data import DataLoader
import pandas as pd
import os
from function.indicators import increase_indicators
import para
import datetime


def build_price_data(target_stocks_list_name, start_date, end_date):

    # 股票清單
    stock_id_list = pd.read_csv(
        os.getcwd()+'/'+target_stocks_list_name, dtype={'stock_id': str}, index_col=False)
    # 逐一去撈，並存為EXCEL
    for index, d in stock_id_list.iterrows():
        stock_id = d['stock_id']
        stock_name = d['stock_name']
        print(stock_id, stock_name)
        data = DataLoader().taiwan_stock_daily(stock_id, start_date, end_date)
        if data.empty == True:
            continue
        # print(data)
        data = increase_indicators(data)

        filename = stock_id+'_'+stock_name + '.csv'
        data.to_csv(os.getcwd()+'/price_record/'+filename,
                    encoding='utf-8-sig', index=False)
        print('build_price_data finished')


if __name__ == '__main__':

    target_stocks_list_name = 'tracing_stock_list.csv'
    # lost_stock_list_name = 'lost_stock_list.csv'
    start_date = '2015-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    build_price_data(target_stocks_list_name, start_date, end_date)

    # 建立檔名列表
    stock_file_list = pd.DataFrame(os.listdir(os.getcwd()+para.price_dir))
    filename_list = 'stock_filename_list.csv'
    stock_file_list.to_csv(filename_list, index=False,
                           encoding='utf-8-sig')
