from FinMind.data import DataLoader
import pandas as pd
import os
import function_pack.file_dir as file_dir
import datetime
import function 
from function_pack.api_limit_error import api_upper_limit_error

def cumulative_revenue_growth(df):
    df['date'] = pd.to_datetime(df['date'])
    # 根据'year'和'month'分组，计算每个月份的总收入
    df['cumulative_revenue'] = df.rolling(12)['revenue'].sum()
    # 计算每个月份的累积增长百分比（相对于去年同月的累积收入）
    df['cumulative_growth_percentage'] = ((df['cumulative_revenue'] - df['cumulative_revenue'].shift(12)) / df['cumulative_revenue'].shift(12)) 

    return df

@api_upper_limit_error
def call_revenue_data(stock_id, start_date, end_date):
    # CALL營收API
    data = DataLoader().taiwan_stock_month_revenue(stock_id, start_date, end_date)
    return data

def build_revenue_data(target_stocks_list_name, start_date, end_date):
    
    # 股票檔名清單to name & id 
    stock_list=function.import_filename_list(target_stocks_list_name)
    # 逐一去撈，並存為EXCEL
    for index, row in stock_list.iterrows():
        stock_id = row['stock_id']
        stock_name = row['stock_name']
        print(stock_id, stock_name)
        # CALL Finmind Api
        data = call_revenue_data(stock_id, start_date, end_date)
        if data.empty == True:
            print('no revenue data')
            continue
        #增加累積營收年增率
        data = cumulative_revenue_growth(data)
        data.to_csv(os.getcwd()+file_dir.revenue_dir+row['file_name'],
                    encoding='utf-8-sig', index=False)
        print('build_revenue_data finished')



def main():
    target_stocks_list_name = 'stock_filename_list.csv'
    start_date = '2015-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    build_revenue_data(target_stocks_list_name, start_date, end_date)


if __name__ == '__main__':
    main()

