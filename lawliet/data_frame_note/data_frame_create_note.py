# @Time    : 2020/11/4 3:29 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : data_frame_query_note.py
# @Software: PyCharm
# @Description: dataFrame 创建

import pandas as pd
import numpy as np


"""
DataFrame是一种表格型数据结构，它含有一组有序的列，每列可以是不同的值。DataFrame既有行索引，也有列索引，它可以看作是由Series组成的字典，不过这些Series公用一个索引。
DataFrame的创建有多种方式，不过最重要的还是根据dict进行创建，以及读取csv或者txt文件来创建。这里主要介绍这两种方式。

作者：文哥的学习日记
链接：https://www.jianshu.com/p/8024ceef4fe2
来源：简书
著作权归作者所有。商业转载请联系作者获得授权，非商业转载请注明出处。
"""
### 根据字典创建
data = {
    'state':['Ohio','Ohio','Ohio','Nevada','Nevada'],
    'year':[2000,2001,2002,2001,2002],
    'pop':[1.5,1.7,3.6,2.4,2.9]
}
frame = pd.DataFrame(data)
print(frame)
frame= frame.append([{'state':'add','year':212}], ignore_index=True)
print(frame)


print("############frame2#################")
### DataFrame的行索引是index，列索引是columns，我们可以在创建DataFrame时指定索引的值：
frame2 = pd.DataFrame(data,index=['one','two','three','four','five'],columns=['year','state','pop','debt'])
print(frame2)


### 使用嵌套字典也可以创建DataFrame，此时外层字典的键作为列，内层键则作为索引:
pop = {'Nevada':{2001:2.4,2002:2.9},'Ohio':{2000:1.5,2001:1.7,2002:3.6}}
frame3 = pd.DataFrame(pop)
print(frame3)


people=pd.DataFrame(np.random.randn(5,5),
                 columns=['a','b','c','d','e'],
                 index=['Joe','Steve','Wes','Jim','Travis'])
print(people)

frame = pd.DataFrame(np.arange(8).reshape((2,4)),index=['three','one'],columns=['d','a','b','c'])
print(frame)

print(np.arange(8))
print(np.arange(8).reshape((2,4)))
print(np.arange(8).reshape((2,4)).reshape((4,2)))
