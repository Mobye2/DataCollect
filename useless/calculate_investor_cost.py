import pandas as pd
import function.file_dir as file_dir
import os
#Generate a function calculate  the cost of investors
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
    



# if __name__ == '__main__':
    # file_list_name = 'tracing_stock_list.csv'
    # stock_id_list = pd.read_csv(os.getcwd()+'/'+file_list_name, dtype={'stock_id': str}, index_col=False)
    # for index, d in stock_id_list.iterrows():
    #     stock_id = d['stock_id']
    #     stock_name = d['stock_name']
    #     print(stock_id, stock_name)
    #     #chips_price_data=calculate_cost(stock_id+'_'+stock_name+'.csv')
        #chips_price_data.to_csv(os.getcwd()+file_dir.test_dir+stock_id+'_'+stock_name+'.csv', index=False)
