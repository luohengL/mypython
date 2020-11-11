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



frame = pd.DataFrame(np.arange(9).reshape((3,3)),index = ['a','c','d'],columns = ['Ohio','Texas','California'])
print(frame)

frame = frame.drop('a')
print(frame)


frame = frame.drop(['Ohio'],axis=1)
print(frame)



