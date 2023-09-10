from FinMind.data import DataLoader
import pandas as pd
import os
import time
import function.file_dir as file_dir
import datetime
from function import api_upper_limit_error

def investor_data_transformer(data: pd.DataFrame):
    new = {
        'date': [],
        'stock_id': [],
        'Foreign_Investor_Buy': [],
        'Foreign_Investor_Sell': [],
        'Investment_Trust_Buy': [],
        'Investment_Trust_Sell': [],
        'Dealer_Self_Buy': [],
        'Dealer_Self_Sell': [],
        'Dealer_Hedging_Buy': [],
        'Dealer_Hedging_Sell': [],
    }
    new = pd.DataFrame(new)
    new['stock_id'] = 2  # data.at[1, 'stock_id']
    dates = data['date']

    dates.drop_duplicates(keep='first', inplace=True)
    dates.reset_index(drop=True, inplace=True)
    # print(dates)

    foreign_investment = data.loc[data['name'] == 'Foreign_Investor']
    foreign_investment.set_index('date', drop=True, inplace=True)
    Investment_Trust = data.loc[data['name'] == 'Investment_Trust']
    Investment_Trust.set_index('date', drop=True, inplace=True)
    Dealer_Self = data.loc[data['name'] == 'Dealer_Self']
    Dealer_Self.set_index('date', drop=True, inplace=True)
    Dealer_Hedging = data.loc[data['name'] == 'Dealer_Hedging']
    Dealer_Hedging.set_index('date', drop=True, inplace=True)

    # date = 逐日指定日  stock_id = 指定ID  'Foreign_Investor_Buy' = 篩選date+name 的buy值
    for index, p in data.iterrows():
        pass
    for i in range(0, len(dates)):
        new.at[i, 'date'] = dates.at[i]
        new.at[i, 'Foreign_Investor_Buy'] = foreign_investment.at[dates.at[i], 'buy']
        new.at[i, 'Foreign_Investor_Sell'] = foreign_investment.at[dates.at[i], 'sell']
        new.at[i, 'Investment_Trust_Buy'] = foreign_investment.at[dates.at[i], 'buy']
        new.at[i, 'Investment_Trust_Sell'] = foreign_investment.at[dates.at[i], 'sell']
        new.at[i, 'Dealer_Self_Buy'] = foreign_investment.at[dates.at[i], 'buy']
        new.at[i, 'Dealer_Self_Sell'] = foreign_investment.at[dates.at[i], 'sell']
        new.at[i, 'Dealer_Hedging_Buy'] = foreign_investment.at[dates.at[i], 'buy']
        new.at[i, 'Dealer_Hedging_Sell'] = foreign_investment.at[dates.at[i], 'sell']
    new['stock_id'] = data.at[1, 'stock_id']
    return new

@api_upper_limit_error
def call_chip_data(stock_id, start_date, end_date):
    # CALL融資融券、三大法人API，並合併
    margin_data = DataLoader().taiwan_daily_short_sale_balances(stock_id, start_date, end_date)
    time.sleep(2)
    investor_data = DataLoader().taiwan_stock_institutional_investors(stock_id, start_date, end_date)
    investor_data = investor_data_transformer(investor_data)   # 三大法人買賣超日報轉為逐日資料
    chips_data = margin_data.merge(investor_data, on=['date', 'stock_id'])
    return chips_data

def calculate_investor_cost(stock_filename,chips_data):
    # import the data from the file
    price=pd.read_csv(os.getcwd()+file_dir.price_dir+stock_filename, dtype={'stock_id': str}, index_col=False)
    price=price[['date', 'stock_id', 'close']]
    #merge the data
    chips_price_df = pd.merge(chips_data,price,how='left',on=['date','stock_id'])

    # Calculate the net volume for each day
    chips_price_df['net_volume'] = chips_price_df['Investment_Trust_Buy'] - chips_price_df['Investment_Trust_Sell'] +chips_price_df['Foreign_Investor_Buy']-chips_price_df['Foreign_Investor_Sell']+chips_price_df['Dealer_Self_Buy']-chips_price_df['Dealer_Self_Sell']
    # Initialize a list to store the calculated cumulative costs
    cumulative_costs = []
    cumulative_net_volumes = []
    average_holding_costs = []
    # Calculate the cumulative cost for each day based on volume and price
    cumulative_cost = 0
    cumulative_net_volume=0
    last_average_holding_cost=0
    for index, row in chips_price_df.iterrows():
        #calculate cumulative net volume
        cumulative_net_volume+=row['net_volume']
        cumulative_net_volume=max(cumulative_net_volume, 0)
        cumulative_net_volumes.append(cumulative_net_volume)
        #if cumulative net volume <0, use last average holding cost,because not tracking the margin, only the cost right now
        if row['net_volume'] >= 0:
            cumulative_cost += (row['net_volume'] * row['close'])
        else:
            cumulative_cost += (row['net_volume'] * last_average_holding_cost)
        cumulative_cost=max(cumulative_cost, 0)
        # Add the calculated cumulative cost to the list
        cumulative_costs.append(cumulative_cost)
        #calculate average holding cost, also make sure not divide by 0
        try:
            last_average_holding_cost=cumulative_cost/cumulative_net_volume
        except:
            last_average_holding_cost=0
        average_holding_costs.append(last_average_holding_cost)
        

    # Add the calculated cumulative costs to the DataFrame
    chips_price_df['cumulative_cost'] = cumulative_costs
    chips_price_df['cumulative_net_volume']=cumulative_net_volumes
    chips_price_df['average_holding_cost']=average_holding_costs

    # Calculate the average holding cost for each day
    #df['average_holding_cost'] = df['cumulative_cost'] / df['cumulative_net_volume']

    #make average holding cost *(-1) if cumulative net volume <0
    chips_price_df.loc[chips_price_df['cumulative_net_volume'] < 0, 'average_holding_cost'] = chips_price_df['average_holding_cost'] * (-1)
    # Save the DataFrame to a CSV file
    average_holding_costs=chips_price_df['average_holding_cost']
    return average_holding_costs

def build_margin_data(target_stocks_list_name, start_date, end_date):
    # 股票名清單split 'filename' into the stock_id and stock_name    
    stock_id_list = pd.read_csv(os.getcwd()+'/'+target_stocks_list_name, index_col=False)
    stock_id_list[['stock_id','stock_name']] = stock_id_list['file_name'].str.split('_',expand=True)
    stock_id_list[['stock_name','extension']] = stock_id_list['stock_name'].str.split('.',expand=True)

    # 逐一去撈，並存為EXCEL
    for index, filename in stock_id_list.iterrows():
        print (filename['stock_id'], filename['stock_name'])
        chips_data=call_chip_data(filename['stock_id'], start_date, end_date)
        #add法人average holding cost 
        average_holindcost=calculate_investor_cost(filename['file_name'],chips_data)
        chips_data['average_holding_cost']=average_holindcost
        #save data
        chips_data.to_csv(os.getcwd()+file_dir.chip_dir + filename['file_name'],
                    encoding='utf-8-sig', index=None)
        print('chips_data finished')



if __name__ == '__main__':

    target_stocks_list_name = 'stock_filename_list.csv'
    start_date = '2015-01-01'
    end_date = datetime.datetime.now().strftime('%Y-%m-%d')
    build_margin_data(target_stocks_list_name, start_date, end_date)
