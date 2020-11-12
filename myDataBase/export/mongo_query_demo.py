# @Time    : 2020/11/11 6:19 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : mongo_query_demo.py
# @Software: PyCharm
# @Description: mongo 查询demo

from mongo_data_factory import MongoDataBaseFactory as dbf
from mongo_data_factory import pro_db_config as db_config
import pandas as pd

dbf = dbf.MongoDataBaseFactory(db_config)
db = dbf.getdb()
data = db.policy.find({"orderTime" :{"$gte": "1595543167000","$lte": "1598221567000"}},{"_id":0,"fusePolicyCode":1,"mobileName":1,"productCode":1,"productName":1,"policyAmount":1})

data_list = [u for u in data]
frame = pd.DataFrame(data_list)
print(frame)

print("finish.....")

## 关闭连接
dbf.shoutdown()