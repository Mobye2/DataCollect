import pandas as pd
import os

def fixColumn(target_stocks_list_name):

    # 追蹤股票清單, 檔名拆為id & name
    stock_id_list = pd.read_csv(os.getcwd()+'/'+target_stocks_list_name, index_col=False)
    stock_id_list[['stock_id','stock_name']] = stock_id_list['file_name'].str.split('_',expand=True)
    stock_id_list[['stock_name','extension']] = stock_id_list['stock_name'].str.split('.',expand=True)
    # 依清單逐一去撈，並存為EXCEL
    for index, filename in stock_id_list.iterrows():
        
        stock_id = filename['stock_id']
        stock_name = filename['stock_name']
        print(stock_id, stock_name)
        try:
            price_data=pd.read_csv(os.getcwd()+'/Data/eps_record/'+stock_id+'_'+stock_name+".csv", dtype={'stock_id': str}, index_col=False)
        except:
            continue
        
        price_data = price_data.rename(columns={'high': 'High', 'low': 'Low'})
        price_data = price_data.rename(columns={'open': 'Open', 'close': 'Close', 'volume': 'Volume','Trading_volume': 'Volume','5MA': 'five_MA','10MA': 'ten_MA','20MA': 'twenty_MA'})
        #存檔
        price_data.to_csv(os.getcwd()+'/Data/eps_record/'+filename['file_name'],
                    encoding='utf-8-sig', index=False)


if __name__ == '__main__':

    #target_stocks_list_name = 'tracing_stock_list.csv'
    stock_filemame_list = 'stock_filename_list.csv'
    fixColumn(stock_filemame_list)