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

## 排序
print("##########sort##########")
frame = pd.DataFrame(np.arange(20).reshape((5,4)),index=['one','two','three','four','five'],columns=['d','a','b','c'])
print(frame)

frame1 = frame.sort_index()
print(frame1)

frame2 = frame.sort_index(1,ascending=False)
print(frame2)