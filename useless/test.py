import update_dailyuseless
import sys
print (sys.path)
from .history_price_api import tsf

tsf()

list = []
for i in range(-5, 0):
    index = -i
    print(i, index)
    list.append(i)
print(list)
print(list[-2])
