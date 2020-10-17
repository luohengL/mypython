import pymongo
import atexit
import paramiko
from sshtunnel import SSHTunnelForwarder



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

data = db.basicData.find_one()
print(data)
client.close()
def shutdown():
    server.stop()
atexit.register(shutdown)
server.close()
print("server close....")