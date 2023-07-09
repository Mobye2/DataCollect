import pandas as pd
import update_daily.file_dir as file_dir
from FinMind.data import DataLoader
import datetime
import os
from build_history.inves_data_trans import investor_data_transformer
from indicators import increase_indicators


def before_end():
    date = datetime.datetime.now()
    end_time = datetime.datetime.strptime('14:00:00', "%H:%M:%S")
    real_date = (date.hour - end_time.hour) < 0
    return real_date

# 更新每日股價、融資融券、三大法人
# for 所有股票
#     每日股價


def daily_price_update(filename, today_str, stock_id):
    history_price_data = pd.read_csv(
        os.getcwd()+file_dir.STAlert+file_dir.price_dir+filename, encoding='utf-8-sig', dtype={'stock_id': str})
    # 若重複結束
    last_date = history_price_data.iloc[-1].at['date']
    if last_date == today_str:
        print('repeat')
    # 撈取今日資料並合併
    else:
        new_price_data = DataLoader().taiwan_stock_daily(stock_id, last_date, today_str)
        if new_price_data.empty == True:
            return
        new_price_data = new_price_data.rename(columns={'max': 'high', 'min': 'low'})
        price_data = pd.concat([history_price_data, new_price_data]).reset_index(drop=True)
        price_data = increase_indicators(price_data)

        price_data.to_csv(os.getcwd()+file_dir.STAlert+file_dir.price_dir+filename,
                          encoding='utf-8-sig', index=None)


def daily_chip_update(filename, today_str, stock_id):
    # 取歷史股價資料準備更新
    history_chip_data = pd.read_csv(
        os.getcwd()+file_dir.STAlert+file_dir.chip_dir+filename, encoding='utf-8-sig', dtype={'stock_id': str})
    # 若重複結束
    last_date = history_chip_data.iloc[-1].at['date']
    if last_date == today_str:
        print('repeat')
    else:
        # 撈取今日資料並合併
        # 融資融券API
        margin_data = DataLoader().taiwan_daily_short_sale_balances(stock_id, last_date, today_str)
        # 三大法人API
        investor_data = DataLoader().taiwan_stock_institutional_investors(stock_id, last_date, today_str)
        investor_data = investor_data_transformer(investor_data)  # 轉換格式
        if margin_data.empty == True or investor_data.empty == True:
            return
        # 左右合併
        chips_data = margin_data.merge(investor_data, on=['date', 'stock_id'])
        # 新舊合併
        chips_data = pd.concat([history_chip_data, chips_data]).reset_index(drop=True)
        chips_data_date = chips_data['date']
        chips_data = chips_data.drop('date', axis=1)
        chips_data.insert(0, 'date', chips_data_date)
        path = os.getcwd()+file_dir.STAlert
        chips_data.to_csv(path+'/chips_record/'+filename,
                          encoding='utf-8-sig', index=None)


def revenue_update(filename, stock_id):
    start_date = '2022-01-01'
    revenue = DataLoader().taiwan_stock_month_revenue(stock_id, start_date)
    if revenue.empty == True:
        print('revenue empty')
        # ETF has no revenue(empty)
        return
    last_date = revenue.at[(len(revenue)-1), 'date']
    if last_date[0:7] == start_date[0:7]:
        print('revenue repeat')
        pass
    else:
        print(os.getcwd()+file_dir.STAlert+file_dir.revenue_dir+filename[0])
        revenue.to_csv(os.getcwd()+file_dir.STAlert+file_dir.revenue_dir+filename,
                       encoding='utf-8-sig', index=None)


if __name__ == '__main__':
    print(os.getcwd()+file_dir.STAlert+'/stock_file_list.csv')
    filename_list = pd.read_csv(os.getcwd()+file_dir.STAlert+'/stock_file_list.csv', encoding='utf-8-sig')
    today = datetime.datetime.now()  # 跑前一天，否則資料可能空白
    today_str = datetime.datetime.strftime(today, '%Y-%m-%d')

    # 依列表更新個股
    for index, filename in filename_list.iterrows():
        stock_id, stock_name_csv = filename[0].split('_')
        print(stock_id, stock_name_csv)
    # 股價資料
        daily_price_update(filename[0], today_str, stock_id)
    # 融資融券、三大法人
        daily_chip_update(filename[0], today_str, stock_id)
    # 月營收表

        if datetime.datetime.now().day <= 11:
            print('update revenue')
            revenue_update(filename[0], stock_id)
        else:

            pass
