import pymongo
import pandas as pd
import atexit
import paramiko
from sshtunnel import SSHTunnelForwarder
from datetime import datetime, timedelta,time



private_key = paramiko.RSAKey.from_private_key_file("/Users/luoheng/Documents/fuse/key/id_rsa_db")
# 连接database
server = SSHTunnelForwarder(
    # 指定ssh登录的跳转机的address
    ssh_address_or_host=('47.74.249.151', 22018),
    # 设置密钥
    ssh_pkey=private_key,
    # 如果是通过密码访问，可以把下面注释打开，将密钥注释即可。
    # ssh_password = "password"
    # 设置用户
    ssh_username='user001',
    # 设置数据库服务地址及端口
    remote_bind_address=('dds-k1a47be55c9d48141.mongodb.ap-southeast-5.rds.aliyuncs.com', 3717))

server.start()
print("server started....")

client = pymongo.MongoClient(host=server.local_bind_host,port= server.local_bind_port)
db = client.fuse
db.authenticate(name='cafl', password='cArfinAncia1')
print(client)

data = db.policy.find({"$or":[{"cancelTime":{ "$exists": "true" }},{"forceCancelTime":{ "$exists": "true" }}], "fusePolicyCode":{
        "$in":["MAG-20200102-050600001234483",
"ADIRA-20200102-064300038435007",
"ADIRA-20200103-022500000667681"]}},{"_id":0,"fusePolicyCode":1,"cancelTime":1,"forceCancelTime":1})

data_list = [u for u in data]
print(data_list)
frame = pd.DataFrame(data_list, columns=['fusePolicyCode', 'cancelTime','forceCancelTime'])
print(frame)





t = datetime.now().date() - timedelta(days=1)
writer = pd.ExcelWriter("cancelTime2" + (u'_%d%02d%02d.xlsx' % (t.year, t.month, t.day)))

wb = writer.book

frame.to_excel(writer, sheet_name=u'rejectEditLog', encoding='utf8', header=True, index=False, startcol=0, startrow=0)
writer.save()



## 关闭连接
def shutdown():
    server.stop()

client.close()
atexit.register(shutdown)
server.close()
print("server close....")