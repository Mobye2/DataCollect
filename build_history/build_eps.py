from FinMind.data import DataLoader
import os
import function.file_dir as file_dir
import pandas as pd

def calculate_eps(stock_id,stock_name,start_date):

    api = DataLoader()
    # api.login_by_token(api_token='token')
    # api.login(user_id='user_id',password='password')
    #取得淨值比PBR 本益比PER 現金殖利率dividend_yield  現金流量表CashFlow
    PBR = api.taiwan_stock_per_pbr(stock_id,start_date)
    #資產負債表BalnaceSheet_應付帳款AccountPayable 負債
    BalnaceSheet= api.taiwan_stock_balance_sheet(stock_id,start_date)
    BalnaceSheet=BalnaceSheet[BalnaceSheet['type']=='AccountsPayable']
    BalnaceSheet.rename(columns={'value': 'AccountsPayable'}, inplace=True)
    BalnaceSheet=BalnaceSheet[['date', 'stock_id', 'AccountsPayable']]
    #現金流量表CashFlow_CashBalanceIncrease現金增加
    CashFlow= api.taiwan_stock_cash_flows_statement(stock_id,start_date)
    CashFlow=CashFlow[CashFlow['type']=='CashFlowsFromOperatingActivities']
    CashFlow.rename(columns={'value': 'CashFlows'}, inplace=True)
    CashFlow=CashFlow[['date', 'stock_id', 'CashFlows']]
    #股利政策表 TaiwanStockDividend 取得股票數量
    Stockdivid = api.taiwan_stock_dividend(stock_id,start_date)
    Stockdivid=Stockdivid[['date', 'stock_id', 'ParticipateDistributionOfTotalShares']]
    Stockdivid.rename(columns={'ParticipateDistributionOfTotalShares': 'TotalShares'}, inplace=True)

    #取得EPS資料新增EPS欄位
    data = api.taiwan_stock_financial_statement(stock_id,start_date)
    data=data[data['type']=='EPS']
    data.rename(columns={'value': 'Eps'}, inplace=True)
    data['last_year_eps']=data['Eps'].rolling(4).sum()
    data=data[['date', 'stock_id', 'Eps', 'last_year_eps']]
    print('build_eps_data finished')

    #Concat with price data , fill price to the row with eps 
    price_data=pd.read_csv(os.getcwd()+file_dir.price_dir+stock_id+'_'+stock_name+".csv", dtype={'stock_id': str}, index_col=False)
    price_data=pd.merge(price_data,PBR,on=['date','stock_id'],how='outer')
    price_data=pd.merge(price_data,CashFlow,on=['date','stock_id'],how='outer')
    price_data=pd.merge(price_data,Stockdivid,on=['date','stock_id'],how='outer')
    result_df = pd.merge(data, price_data, on=['date','stock_id'],how='outer')
    result_df.sort_values(by='date', inplace=True)  # Sort the DataFrame by the 'date' column in ascending order
    # Use fillna method to fill missing 'price' data with the last row's 'price'
    #result_df['close'].fillna(method='ffill', inplace=True)
    result_df['Eps'].fillna(method='ffill', inplace=True)
    result_df['TotalShares'].fillna(method='ffill', inplace=True)
    result_df['CashFlows'].fillna(method='ffill', inplace=True)
    result_df['last_year_eps'].fillna(method='ffill', inplace=True)
    #現金流量比OCF
    result_df['OCF']=result_df['CashFlows']*4/result_df['TotalShares']
    result_df['empty_price'] = result_df['close'].isnull()
    #drop the row with empty price
    result_df=result_df[result_df['empty_price']==False]
    #print (result_df)
    return result_df

if __name__ == '__main__':
    start_date='2019-01-01'

    file_list_name = 'tracing_stock_list.csv'
    stock_id_list = pd.read_csv(os.getcwd()+'/'+file_list_name, dtype={'stock_id': str}, index_col=False)
    for index, d in stock_id_list.iterrows():
        stock_id = d['stock_id']
        stock_name = d['stock_name']
        print(stock_id, stock_name)
        result_df=calculate_eps(stock_id,stock_name,start_date)

        filename = stock_id+'_'+stock_name + '.csv'
        result_df.to_csv(os.getcwd()+"/Data/eps_record/"+filename,
                encoding='utf-8-sig', index=False)
