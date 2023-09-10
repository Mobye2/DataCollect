import os
import pandas as pd

def import_filename_list(target_stocks_list_name):
    # 追蹤股票清單, 檔名拆為id & name
    stock_id_list = pd.read_csv(os.getcwd()+'/'+target_stocks_list_name, index_col=False)
    stock_id_list[['stock_id','stock_name']] = stock_id_list['file_name'].str.split('_',expand=True)
    stock_id_list[['stock_name','extension']] = stock_id_list['stock_name'].str.split('.',expand=True)
    return stock_id_list