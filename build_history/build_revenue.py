from FinMind.data import DataLoader
import pandas as pd
import os
from function.indicators_test import increase_indicators
import function.file_dir as file_dir
import datetime

def cumulative_revenue_growth(df):
    df['date'] = pd.to_datetime(df['date'])
    # 根据'year'和'month'分组，计算每个月份的总收入
    df['cumulative_revenue'] = df.rolling(12)['revenue'].sum()
    # 计算每个月份的累积增长百分比（相对于去年同月的累积收入）
    df['cumulative_growth_percentage'] = ((df['cumulative_revenue'] - df['cumulative_revenue'].shift(12)) / df['cumulative_revenue'].shift(12)) 

    return df

def build_revenue_data(target_stocks_list_name, start_date, end_date):

    # 股票清單
    stock_id_list = pd.read_csv(
        os.getcwd()+'/'+target_stocks_list_name, dtype={'stock_id': str}, index_col=False)
    # 逐一去撈，並存為EXCEL
    for index, d in stock_id_list.iterrows():
        stock_id = d['stock_id']
        stock_name = d['stock_name']
        print(stock_id, stock_name)
        # CALL Finmind Api
        data = DataLoader().taiwan_stock_month_revenue(stock_id, start_date, end_date)
        if data.empty == True:
            print('no revenue data')
            continue
        # print(data)
        data = cumulative_revenue_growth(data)
        print (data)


        filename = stock_id+'_'+stock_name + '.csv'
        data.to_csv(os.getcwd()+file_dir.revenue_dir+filename,
                    encoding='utf-8-sig', index=False)
        print('build_revenue_data finished')




def main():
    target_stocks_list_name = 'tracing_stock_list.csv'
    lost_stock_list_name = 'lost_stock_list.csv'
    start_date = '2015-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    build_revenue_data(target_stocks_list_name, start_date, end_date)


if __name__ == '__main__':
    main()

