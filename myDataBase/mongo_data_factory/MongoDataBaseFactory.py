# @Time    : 2020/11/11 6:11 下午
# @Author  : luoh
# @Email   : luohenghlx@163.com
# @File    : MongoDataBaseFactory.py
# @Software: PyCharm
# @Description: mongodb 数据库连接工厂类

import pymongo
import atexit
import paramiko
from sshtunnel import SSHTunnelForwarder



class MongoDataBaseFactory:

    def __init__(self, db_config):
        print('数据库初始化。。。。。')
        configs = db_config
        print(configs.host)
        if (hasattr(configs, 'use_ssh') and configs.use_ssh == 'yes'):
            self.connect_db_by_ssh(configs)
        else:
            self.connect_db(configs)



    def connect_db(self, db_config):
        print("数据库建立连接。。。。。。")
        configs = db_config
        # 连接database
        try:
            client = pymongo.MongoClient(host=configs.host, port=configs.port)
            ## connect=True，说明你已经连接成功了
            print(client)
            self.condb = client[configs.database]
            self.condb.authenticate(name=configs.user, password=configs.password)
            print("success......")
        except Exception as abnormal:
            print("数据库连接错误，错误内容%s " % abnormal)
        return self.condb

    def connect_db_by_ssh(self, db_config):
        print("连接 ssh。。。。。。")
        configs = db_config
        private_key = paramiko.RSAKey.from_private_key_file(configs.private_key)
        # 连接database
        self.server = SSHTunnelForwarder(
            # 指定ssh登录的跳转机的address
            ssh_address_or_host=(configs.ssh_host, configs.ssh_port),
            # 设置密钥
            ssh_pkey=private_key,
            # 如果是通过密码访问，可以把下面注释打开，将密钥注释即可。
            # ssh_password = "password"
            # 设置用户
            ssh_username=configs.ssh_username,
            # 设置数据库服务地址及端口
            remote_bind_address=(configs.host, configs.port))
        self.server.start()
        try:
            print("数据库建立连接。。。。。。")
            client = pymongo.MongoClient(host=self.server.local_bind_host,port= self.server.local_bind_port)
            ## connect=True，说明你已经连接成功了
            print(client)
            self.condb = client[configs.database]
            self.condb.authenticate(name=configs.user, password=configs.password)
            print("success......")
        except Exception as abnormal:
            print("数据库连接错误，错误内容%s " % abnormal)

        return self.db

    ## 获取数据库连接
    def getConn(self):
        return self.condb
    ## 关闭连接
    def shutdown(self):
        self.server.stop()

    def __del__(self):
        if(hasattr(self, 'server')):
            self.server.stop()
            self.server.close()