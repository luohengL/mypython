# @Time    : 2020/11/4 3:29 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : data_frame_query_isin_note.py
# @Software: PyCharm
# @Description: DataFrame 查询


import pandas as pd
import numpy as np

data = {
    'state':['Ohio','Ohio','Ohio','Nevada','Nevada'],
    'year':[2000,2001,2002,2001,2002],
    'pop':[1.5,1.7,3.6,2.4,2.9]
}
frame = pd.DataFrame(data)
print(frame)
print('#####################')

frame.loc[frame['year'].isin([2000,2001]),['pop']]=frame.apply(lambda  x : x['pop']*100,axis=1)

print(frame)
print('#####################')
frame = frame.loc[frame['year'].isin([2000,2001]),:]

print(frame)