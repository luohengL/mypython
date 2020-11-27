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
data = db.sysRoleModule.find({"moduleStr":{"$regex":"exportBackdoorBtn"},"roleId":{"$ne":"20200"}},{"_id":0,"roleId":1})

data_list = [ObjectId(u['roleId']) for u in data]

print(data_list)
role_data = db.sysRole.find({"_id":{"$in":data_list}},{"_id":0,"roleName":1})

role_data_list = [u for u in role_data]
frame = pd.DataFrame(role_data_list)
print(frame)

print("finish.....")

writer = pd.ExcelWriter("role_list.xlsx")

wb = writer.book

frame.to_excel(writer, sheet_name=u'role_list', encoding='utf8', header=True, index=False, startcol=0, startrow=0)
writer.save()
## 关闭连接
dbf.shoutdown()