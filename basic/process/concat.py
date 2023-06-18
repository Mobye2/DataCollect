import pandas as pd
import os


# filename = 'con_cate.csv'


def split(filename):
    category = pd.read_csv(os.getcwd()+'/basic/'+filename, encoding='utf-8-sig', dtype={'stock_id': str})
    print(category)
    for index, d in category.iterrows():
        category.at[index, 'stock_id'] = d['stockidname'][:4]
        category.at[index, 'stock_name'] = d['stockidname'][4:]
    return category


def main():
    basic = pd.read_csv(os.getcwd()+'/Stock_basic.csv', encoding='utf-8-sig', dtype={'stock_id': str})
    # stock_file_list = pd.DataFrame(os.listdir(os.getcwd()+'/basic/'))
    categories = pd.DataFrame()
    # print(stock_file_list)
    # for index, per in stock_file_list.iterrows():
    # print(per[0])
    # filename = per[0]

    # 股名與類型對應表 進行合併
    filename = 'con_cate.csv'
    category = split(filename)
    category = pd.merge(basic, category, on=['stock_id', 'stock_name'], how='inner')
    categories = pd.concat([categories, category], ignore_index=True)

    print(category)
    print(categories)
    categories = categories.drop(['market', 'industry', 'stockidname'], axis=1)
    basic = pd.merge(basic, categories, on=['stock_id', 'stock_name'], how='left')
    print(basic)
    basic.to_csv('stocks_basic_type.csv', index=False,
                 encoding='utf-8-sig')


if __name__ == "__main__":
    main()
