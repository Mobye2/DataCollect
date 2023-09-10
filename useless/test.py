import pandas as pd

# 假设有三个数据框 df1、df2 和 df3
data1 = {'A': [1, 2, 3], 'B': [4, 5, 6]}
data2 = {'B': [4, 5, 6], 'D': [10, 11, 12]}
data3 = {'B': [4, 5, 6], 'E': [16, 17, 18]}

df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)
df3 = pd.DataFrame(data3)

# 合并多个数据框
combined_df = pd.concat([df1, df2, df3], ignore_index=True)

# 现在，combined_df 包含了 df1、df2 和 df3 的合并数据
print(combined_df)
