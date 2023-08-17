import pandas as pd
#from . import file_dir
from function import file_dir
import os
#Generate a function calculate  the cost of investors
def calculate_cost(stock_name):
    # import the data from the file
    
    chip = pd.read_csv(os.getcwd()+file_dir.chip_dir+stock_name)
    price=pd.read_csv(os.getcwd()+file_dir.price_dir+stock_name)
    #merge the data
    df = pd.merge(chip,price,how='left',on=['date','stock_id'])
    #print (df)

    # Calculate the net volume for each day
    df['net_volume'] = df['Investment_Trust_Buy'] - df['Investment_Trust_Sell'] +df['Foreign_Investor_Buy']-df['Foreign_Investor_Sell']+df['Dealer_Self_Buy']-df['Dealer_Self_Sell']
    # Initialize a list to store the calculated cumulative costs
    cumulative_costs = []
    cumulative_net_volumes = []
    average_holding_costs = []
    # Calculate the cumulative cost for each day based on volume and price
    cumulative_cost = 0
    cumulative_net_volume=0
    last_average_holding_cost=0
    for index, row in df.iterrows():
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
    df['cumulative_cost'] = cumulative_costs
    df['cumulative_net_volume']=cumulative_net_volumes
    df['average_holding_cost']=average_holding_costs

    # Calculate the average holding cost for each day
    #df['average_holding_cost'] = df['cumulative_cost'] / df['cumulative_net_volume']

    #make average holding cost *(-1) if cumulative net volume <0
    df.loc[df['cumulative_net_volume'] < 0, 'average_holding_cost'] = df['average_holding_cost'] * (-1)
    # Save the DataFrame to a CSV file
    print (df)
    return df
    



if __name__ == '__main__':
    file_list_name = 'tracing_stock_list.csv'
    stock_id_list = pd.read_csv(os.getcwd()+'/'+file_list_name, dtype={'stock_id': str}, index_col=False)
    for index, d in stock_id_list.iterrows():
        stock_id = d['stock_id']
        stock_name = d['stock_name']
        print(stock_id, stock_name)
        chips_price_data=calculate_cost(stock_id+'_'+stock_name+'.csv')
        chips_price_data.to_csv(os.getcwd()+file_dir.test_dir+stock_id+'_'+stock_name+'.csv', index=False)
