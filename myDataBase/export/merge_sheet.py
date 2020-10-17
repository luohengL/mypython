import pandas as pd

iris = pd.read_excel('excel/Car_BI_Summary_20200902_V4.xlsx',None)#读入数据文件
keys = list(iris.keys())
#数据合并
iris_concat = pd.DataFrame()
for i in keys:
    iris1 = iris[i]
    iris_concat = pd.concat([iris_concat,iris1])
iris_concat.to_excel('excel/Car_BI_Summary_20200902_V42.xlsx')#数据保存路径