import pandas as pd

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

### 我们可以用index，columns，values来访问DataFrame的行索引，列索引以及数据值，数据值返回的是一个二维的ndarray
print(frame2.index)
print(frame2.columns)
print(frame2.values)
print(frame2[:2])