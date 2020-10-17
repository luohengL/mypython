import pymongo
import pandas as pd
import pprint

MONGO_URI='mongodb://cafl:cArfinAncia1@47.106.165.115:27017/fuse?authSource=admin&authMechanism=SCRAM-SHA-1'
host = '47.106.165.115'
port = 27017
username = 'admin'
password = '30VpzaBN1#5'


client = pymongo.MongoClient(host=host,port=port, username=username, password=password)
## connect=True，说明你已经连接成功了
print(client)

db = client.fuse

data = db.basicData.find({"fusePolicyCode":"FPG-20201010-035300029943684"})

print(type(data))

print("----------------------------------")
data_list = [u for u in data]
print(type(data_list))
print(data_list)
frame = pd.DataFrame(data_list, columns=['fusePolicyCode', 'orderTime', 'prodcutName','policyAmount'])
print(frame)
writer = pd.ExcelWriter("demossdf" + '.xlsx')

wb = writer.book

frame.to_excel(writer, sheet_name=u'测试页签', encoding='utf8', header=True, index=False, startcol=0, startrow=0)
writer.save()
print("finish......")
