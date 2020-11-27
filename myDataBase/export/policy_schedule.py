# @Time    : 2020/11/11 6:19 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : mongo_query_demo.py
# @Software: PyCharm
# @Description: mongo 查询demo

from mongo_data_factory import MongoDataBaseFactory as dbf
from mongo_data_factory import pro_db_config as db_config
import pandas as pd
from bson.objectid import ObjectId

dbf = dbf.MongoDataBaseFactory(db_config)
db = dbf.getdb()

data_result = db.policy.find({"policySchedule":{ "$exists": "true" }},{"_id":0,"fusePolicyCode":1})

data_list = [u for u in data_result]
frame = pd.DataFrame(data_list)
print(frame)

print("finish.....")

writer = pd.ExcelWriter("policySchedule.xlsx")

wb = writer.book

frame.to_excel(writer, sheet_name=u'policySchedule', encoding='utf8', header=True, index=False, startcol=0, startrow=0)
writer.save()
## 关闭连接
dbf.shoutdown()