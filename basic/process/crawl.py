import random
import time
import requests
from bs4 import BeautifulSoup
import bs4
import os
import pandas as pd
file_list_name = 'infos.csv'
pf = pd.read_csv(os.getcwd()+'/basic/'+file_list_name, dtype={'stock_id': str}, index_col=False)
list = pf['產業分類'].tolist()


def main():

    resp = requests.get('https://www.moneydj.com/Z/ZH/ZHA/ZHA.djhtm')
    soup = BeautifulSoup(resp.text, 'html.parser')
    # 爬所有的產業類型
    rowsa = soup.find_all('td', attrs={'class': "t3t1"})
    rowsb = soup.find_all('td', attrs={'class': "t3t1_rev"})
    rowsa = rowsa+rowsb
    url_list = pd.DataFrame()

    for i in rowsa:
        # 避免對空值運算
        if i.find('a') is None:
            pass
        else:
            # 取得標籤內文:產業類型名稱
            tr = i.find('a').getText()
            # 針對要蒐集的產業類型取的連結
            for name in list:
                if tr == name:
                    h = i.find('a').get('href')
                    # print(h)
                    url_list = url_list.append({'type': tr, 'url': 'https://www.moneydj.com/'+h}, ignore_index=True)

    # 刪除重複資料
    url_list.drop_duplicates(subset=None, keep='first', inplace=True)
    url_list.reset_index(drop=True, inplace=True)

    # 爬產業類型裡面的股票名稱並存成列表(股名、產業類型)
    stock_by_type = stocks_in_url(url_list)
    # 將跨產業的股票，合併產業資訊
    # merge_type(stock_by_type)


def stocks_in_url(url_list: pd.DataFrame):
    stock_by_type = pd.DataFrame()
    for index, name_url in url_list.iterrows():
        type_name = name_url[0]
        url = name_url[1]
        # 爬該產業類型的URL
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, 'html.parser')
        stock_id_names = soup.find_all('td', attrs={'id': "oAddCheckbox"})

        # 取出指定標籤內的股票名
        for name in stock_id_names:
            if name.find('a') is None:
                pass
            else:
                tr = name.find('a').getText()

            # 儲存股票名與相對產業
            stock_by_type = stock_by_type.append({'stockidname': tr, 'type': type_name}, ignore_index=True)
        sleeptime = random.randint(1, 3)
        time.sleep(sleeptime)

    print(stock_by_type)
    stock_by_type.to_csv(type_name+'.csv', index=False,
                         encoding='utf-8-sig')
    return stock_by_type


def merge_type(stock_by_type: pd.DataFrame):
    file_list_name = 'categories.csv'
    stock_by_type = pd.read_csv(os.getcwd()+'/basic/'+file_list_name)
    print(stock_by_type)

    stock_by_type['type'] = stock_by_type['type'].apply(lambda x: '/'+x)

    data = stock_by_type.groupby(by='stockidname').sum()
    data['type'] = data['type'].apply(lambda x: x[1:])

    print(data)
    filename = 'con_cate.csv'
    data.to_csv(filename,
                encoding='utf-8-sig')

    stock_by_type = pd.read_csv(os.getcwd()+'/basic/'+filename)
    print(type(stock_by_type.at[0, 'type']))


if __name__ == '__main__':
    # main()
    file_list_name = 'categories.csv'
    stock_by_type = pd.read_csv(os.getcwd()+'/basic/'+file_list_name)
    merge_type(stock_by_type)
