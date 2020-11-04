# @Time    : 2020/11/4 3:29 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : data_frame_query_note.py
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

### 我们可以用index，columns，values来访问DataFrame的行索引，列索引以及数据值，数据值返回的是一个二维的ndarray
print("frame: ")
print(frame)
print("frame.index: ")
print(frame.index)
print("frame.columns: ")
print(frame.columns)
print("frame.values: ")
print(frame.values)
print("frame.2: ")
print(frame[:2])


print("qie pian")
print(frame['year'])

people=pd.DataFrame(np.random.randn(5,5),
                 columns=['a','b','c','d','e'],
                 index=['Joe','Steve','Wes','Jim','Travis'])
print(people)

frame = pd.DataFrame(np.arange(8).reshape((2,4)),index=['three','one'],columns=['d','a','b','c'])
print(frame)

print(np.arange(8))
print(np.arange(8).reshape((2,4)))
print(np.arange(8).reshape((2,4)).reshape((4,2)))