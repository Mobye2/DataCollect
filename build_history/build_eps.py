from FinMind.data import DataLoader
import os
import function.file_dir as file_dir
import pandas as pd


def calculate_eps(stock_id, stock_name, start_date):
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
    Stockdivid=Stockdivid[['date', 'stock_id','CashEarningsDistribution', 'ParticipateDistributionOfTotalShares']]
    Stockdivid.rename(columns={'ParticipateDistributionOfTotalShares': 'TotalShares','CashEarningsDistribution':'Earnings'}, inplace=True)
    #turn xxxx-xx-xx in to datetime
    Stockdivid['date'] = pd.to_datetime(Stockdivid['date'])
    #sum the 'Earnings' in a year
    for index, row in Stockdivid.iterrows():
        cumulative_earnings = Stockdivid.loc[index, 'Earnings']  # Reset cumulative earnings if it's a new year
        for i in range(1, 5):
            if index - i >= 0 and (row['date'] - Stockdivid.loc[index - i, 'date']).days <= 340:
                cumulative_earnings += Stockdivid.loc[index - i, 'Earnings']
        #new column 'year_earnings' to store the cumulative earnings
        Stockdivid.loc[index, 'year_earnings'] = cumulative_earnings
    #return  datetime to xxxx-xx-xx format
    Stockdivid['date'] = Stockdivid['date'].dt.strftime('%Y-%m-%d')    
     

    #取得EPS資料新增EPS欄位
    eps_data = api.taiwan_stock_financial_statement(stock_id,start_date)
    eps_data=eps_data[eps_data['type']=='EPS']
    eps_data.rename(columns={'value': 'Eps'}, inplace=True)
    eps_data['last_year_eps']=eps_data['Eps'].rolling(4).sum()
    eps_data=eps_data[['date', 'stock_id', 'Eps', 'last_year_eps']]
    #print('build_eps_data finished')

    #累積營收年增率
    revenue = pd.read_csv(os.getcwd()+file_dir.revenue_dir+stock_id+'_'+stock_name+'.csv', dtype={'stock_id': str}, index_col=False)
    revenue=revenue[['date', 'stock_id', 'cumulative_growth_percentage']]


    #Concat with price data 
    price_data=pd.read_csv(os.getcwd()+file_dir.price_dir+stock_id+'_'+stock_name+".csv", dtype={'stock_id': str}, index_col=False)
    price_data=pd.merge(price_data,PBR,on=['date','stock_id'],how='outer')
    price_data=pd.merge(price_data,CashFlow,on=['date','stock_id'],how='outer')
    price_data=pd.merge(price_data,Stockdivid,on=['date','stock_id'],how='outer')
    result_df = pd.merge(eps_data, price_data, on=['date','stock_id'],how='outer')
    result_df = pd.merge(result_df, BalnaceSheet, on=['date','stock_id'],how='outer')
    result_df = pd.merge(result_df, revenue, on=['date','stock_id'],how='outer')

    result_df.sort_values(by='date', inplace=True)  # Sort the DataFrame by the 'date' column in ascending order
    # Use fillna method to fill missing 'price' data with the last row's 'price'
    result_df['Eps'].fillna(method='ffill', inplace=True)
    result_df['TotalShares'].fillna(method='ffill', inplace=True)
    result_df['CashFlows'].fillna(method='ffill', inplace=True)
    result_df['last_year_eps'].fillna(method='ffill', inplace=True)
    result_df['Earnings'].fillna(method='ffill', inplace=True)
    result_df['year_earnings'].fillna(method='ffill', inplace=True)
    result_df['cumulative_growth_percentage'].fillna(method='ffill', inplace=True)

    #現金流量比OCF
    result_df['OCF']=result_df['CashFlows']*4/result_df['TotalShares']

    #殖利率,近6年平均殖利率(不包含空值)
    result_df['dividend_yield']=result_df['year_earnings']/result_df['close']
    result_df['dividend_yield_6y'] = result_df['dividend_yield'].rolling(window=1500, min_periods=1).mean()





    #drop the row with empty price
    result_df['empty_price'] = result_df['close'].isnull()
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
