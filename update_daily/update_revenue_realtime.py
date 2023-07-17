import pandas as pd
import requests
from io import StringIO
import time
import os
import datetime
import function.file_dir as file_dir


def monthly_report(year, month):

    # 假如是西元，轉成民國
    if year > 1990:
        year -= 1911

    url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'_0.html'
    if year <= 98:
        url = 'https://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(year)+'_'+str(month)+'.html'
    print(url)
    # 偽瀏覽器
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

    # 爬該年月的網站，並用pandas轉換成 dataframe
    r = requests.get(url, headers=headers)
    r.encoding = 'big5'
    dfs = pd.read_html(StringIO(r.text), encoding='big-5')
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    df.columns = df.columns.get_level_values(1) #雙重INDEX，只保留第二排INDEX
    print(df)
    df['當月營收'] = pd.to_numeric(df['當月營收'], 'coerce')
    df = df[~df['當月營收'].isnull()]

    df = df[df['公司 代號'] != '合計']
    df = df.reset_index()
    df.drop(df.tail(1).index, inplace=True)
    df.drop('index', axis=1, inplace=True)
    print(df)
    # 偽停頓
    time.sleep(5)
    df = df.rename(columns={'公司 代號': 'stock_id', '公司名稱': 'stock_name', '當月營收': 'revenue', '上月營收': 'last_month_revenue', '去年當月營收': 'last_year_revenue', '上月比較 增減(%)': 'last_month_compare(%)',
                   '去年同月 增減(%)': 'last_year_compare(%)', '當月累計營收': 'accumulated_revenue', '去年累計營收': 'last_year_accumulated_revenue', '前期比較 增減(%)': 'accumulated_revenue_compare', '備註': 'note'})
    df['revenue'] = df['revenue'].apply(lambda x: x * 1000)
    df.to_csv(os.getcwd()+file_dir.revenue_dir+'realtime_revenue.csv', index=False, encoding='utf-8-sig')
    return df

def concat_byname(new_revenue: pd.DataFrame,today):
    stock_list = pd.read_csv(os.getcwd()+'/stock_filename_list.csv', encoding='utf-8-sig', dtype={'stock_id': str})
    for index, file_name in stock_list.iterrows():
        stock_no, stock_name = file_name[0].split('_')
        print(file_name[0])
        keptdata = new_revenue.loc[new_revenue['stock_id'] == stock_no]

        if keptdata.empty:
            print('new data empty, pass')
            continue
        firstday_of_month=datetime.datetime.strftime(today, '%Y-%m-01')
        keptdata = keptdata.assign(date=firstday_of_month)
        print (keptdata)
        # 將各股今日交易結果聯集到歷史資料
        history_data = pd.read_csv(os.getcwd()+file_dir.revenue_dir+file_name[0], thousands=',', dtype={'stock_id': str})
        # 如果資料重複,continue
        if history_data.at[history_data.index[-1], 'revenue'] == keptdata.loc[keptdata.index[-1], 'revenue'] and history_data.at[history_data.index[-1], 'revenue'] == keptdata.loc[keptdata.index[-1], 'revenue']:
            print('repeat')
            continue
        revenue_data = pd.concat([history_data, keptdata], ignore_index=True, join='inner')
        revenue_data.to_csv(os.getcwd()+file_dir.revenue_dir+file_name[0], encoding='utf-8-sig', index=None)
    

if __name__ == "__main__":
    # 民國100年1月
    # monthly_report(100, 1)
    today = datetime.datetime.now()
    today_str = datetime.datetime.strftime(today, '%Y-%m-%d')
    # 西元2011年1月
    if today.day < 30:
        new_revenue=monthly_report(today.year, today.month-1)
        concat_byname(new_revenue,today)
    else:
        # monthly_report(today.year, today.month)
        pass

    