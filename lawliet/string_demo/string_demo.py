# @Time    : 2020/11/30 6:39 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : string_demo.py
# @Software: PyCharm
# @Description:

data = 'hello'
name = 'Tom'
s = f'{name} is doing {data}'
s2 = f'{name} is doing {data}'
print(s)
print(s2)

data = {
    'age':12,
    'hobby':'baseball'
}

s3 = f"{name} is {data['age']} years old ,  hobby is {data['hobby']} and name is {name}"
print(s3)


for k ,v in data.items() :
    print('k:v=',k,v)

for d in data :
    print('d=',d)

new_data = {k.upper() : str(v).upper() for k ,v in data.items() }
print(new_data)

list = ['a','b','c']
new_tuple = ( d.upper() for d in data )
print(tuple(new_tuple))
