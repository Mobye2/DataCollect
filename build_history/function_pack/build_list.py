import os

def build_list(dir_name, filename):
    stock_file_list = os.listdir(os.getcwd()+dir_name)
    stock_file_list.insert(0, 'file_name')
    stock_file_list.to_csv(filename, index=False,
                           encoding='utf-8-sig')
    print ('build_list finished')
