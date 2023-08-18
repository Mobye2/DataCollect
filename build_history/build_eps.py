from FinMind.data import DataLoader
import os
import function.file_dir as file_dir
import pandas as pd


def import_eps(stock_id, stock_name, start_date):
    api = DataLoader()
    # api.login_by_token(api_token='token')
    # api.login(user_id='user_id',password='password')
    # 取得EPS
    data = api.taiwan_stock_financial_statement(
        stock_id,
        start_date,
    )
    data = data[data["type"] == "EPS"]
    data.rename(columns={"value": "Eps"}, inplace=True)
    data["last_year_eps"] = data["Eps"].rolling(4).sum()
    data = data[["date", "stock_id", "Eps", "last_year_eps"]]
    print("build_eps_data finished")

    # 取得淨值比PBR 本益比PER 現金殖利率dividend_yield
    PBR = api.taiwan_stock_per_pbr(stock_id, start_date)

    #merge data and PBR with date and stock_id
    data = pd.merge(data, PBR, on=["date", "stock_id"], how="outer")
    # Concat with price data , fill price to the row with eps
    price_data = pd.read_csv(
        os.getcwd() + file_dir.price_dir + stock_id + "_" + stock_name + ".csv",
        dtype={"stock_id": str},
        index_col=False,
    )
    result_df = pd.merge(data, price_data, on=["date", "stock_id"], how="outer")
    result_df.sort_values(
        by="date", inplace=True
    )  
    # Use fillna method to fill missing 'close'and 'PBR' data with the last row's 'price'
    result_df["empty_price"] = result_df["close"].isnull()
    result_df["close"].fillna(method="ffill", inplace=True)
#    result_df.fillna(method="ffill", inplace=True)

    return result_df



if __name__ == "__main__":
    start_date = "2019-01-01"
    tracing_list = "tracing_stock_list.csv"
    stock_id_list = pd.read_csv(
        os.getcwd() + "/" + tracing_list, dtype={"stock_id": str}, index_col=False
    )
    for index, d in stock_id_list.iterrows():
        stock_id = d["stock_id"]
        stock_name = d["stock_name"]
        print(stock_id, stock_name)
        chips_price_data = import_eps(stock_id, stock_name, start_date)
        chips_price_data.to_csv(
            os.getcwd() + file_dir.eps_dir + stock_id + "_" + stock_name + ".csv",
            index=False,
        )
        break
