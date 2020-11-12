import pymongo
import pandas as pd
import pprint
from mongo_data_factory import newdev_mongo_config as conf
from datetime import datetime, timedelta,time





client = pymongo.MongoClient(host=conf.host,port=conf.port)
db = client[conf.database]
db.authenticate(name=conf.user, password=conf.password)
## connect=True，说明你已经连接成功了
print(client)

data = db.policy.find({"orderTime" :{"$gte": "1595543167000","$lte": "1598221567000"}},{"_id":0,"fusePolicyCode":1,"mobileName":1,"productCode":1,"productName":1,"policyAmount":1})

print(type(data))

print("----------------------------------")
data_list = [u for u in data]
print(type(data_list))
print(data_list)
frame = pd.DataFrame(data_list)
print(frame)





t = datetime.now().date() - timedelta(days=1)
writer = pd.ExcelWriter("mongoExportCancelTime" + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))

wb = writer.book

frame.to_excel(writer, sheet_name=u'测试页签', encoding='utf8', header=True, index=False, startcol=0, startrow=0)
writer.save()
print("finish......")
