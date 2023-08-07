from FinMind.data import DataLoader
import os
import function.file_dir as file_dir
import pandas as pd

api = DataLoader()
stock_id="3231"
start_date='2019-01-01'
# api.login_by_token(api_token='token')
# api.login(user_id='user_id',password='password')

#取得EPS
data = api.taiwan_stock_financial_statement(
    stock_id,
    start_date,
)
data=data[data['type']=='EPS']
data.rename(columns={'value': 'Eps'}, inplace=True)
data['last_year_eps']=data['Eps'].rolling(4).sum()
data=data[['date', 'stock_id', 'Eps', 'last_year_eps']]
print('build_eps_data finished')

#取得淨值比PBR 本益比PER 現金殖利率dividend_yield

PBR = api.taiwan_stock_per_pbr(
    stock_id,
    start_date='2019-01-01'
)



#Concat with price data , fill price to the row with eps 
price_data=pd.read_csv(os.getcwd()+file_dir.price_dir+"/3231_緯創.csv", dtype={'stock_id': str}, index_col=False)
result_df = pd.merge(data, price_data, on=['date','stock_id'],how='outer')
result_df.sort_values(by='date', inplace=True)  # Sort the DataFrame by the 'date' column in ascending order
result_df['empty_price'] = result_df['close'].isnull()
# Use fillna method to fill missing 'price' data with the last row's 'price'
result_df['close'].fillna(method='ffill', inplace=True)
print (result_df)

# chip=pd.read_csv(os.getcwd()+file_dir.chip_dir+"/3231_緯創.csv", dtype={'stock_id': str}, index_col=False)
# price_chip=pd.merge(result_df,chip,on=['date','stock_id'],how='outer')






filename = stock_id + '.csv'
result_df.to_csv(os.getcwd()+"/Data/eps_record/"+filename,
            encoding='utf-8-sig', index=False)
# price_chip.to_csv(os.getcwd()+"/Data/eps_record/pc"+filename,
#             encoding='utf-8-sig', index=False)