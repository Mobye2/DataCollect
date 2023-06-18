import pandas as pd

# 整理撈取的三大法人資料


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
