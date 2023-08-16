import pandas as pd
import function.file_dir as file_dir
import os
#Generate a function calculate  the cost of investors
def calculate_cost():
    # import the data from the file
    stock_name='2330_台積電.csv'
    chip = pd.read_csv(os.getcwd()+file_dir.chip_dir+stock_name)
    price=pd.read_csv(os.getcwd()+file_dir.price_dir+stock_name)
    #merge the data
    df = pd.merge(chip,price,how='left',on=['date','stock_id'])
    print (df)

    # Calculate the net volume for each day
    df['net_volume'] = df['Investment_Trust_Buy'] - df['Investment_Trust_Sell']
    # Calculate the cumulative net volume and cumulative cost
    #df['cumulative_net_volume'] = df['net_volume'].cumsum()
    # Ensure cumulative net volume is at least zero
    #df['cumulative_net_volume'] = df['cumulative_net_volume'].clip(lower=0)
    # Initialize a list to store the calculated cumulative costs
    cumulative_costs = []
    cumulative_net_volumes = []
    average_holding_costs = []
    # Calculate the cumulative cost for each day based on volume and price
    cumulative_cost = 0
    cumulative_net_volume=0
    last_average_holding_cost=0
    for index, row in df.iterrows():
        cumulative_net_volume+=row['net_volume']
        cumulative_net_volume=max(cumulative_net_volume, 0)
        cumulative_net_volumes.append(cumulative_net_volume)
        if row['net_volume'] >= 0:
            cumulative_cost += (row['net_volume'] * row['close'])
        else:
            cumulative_cost += (row['net_volume'] * last_average_holding_cost)
        cumulative_cost=max(cumulative_cost, 0)
        cumulative_costs.append(cumulative_cost)
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


    print(df)
    # Save the DataFrame to a CSV file
    df.to_csv(os.getcwd()+file_dir.test_dir+stock_name, index=False)



if __name__ == '__main__':
    print (calculate_cost())