import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder
from datafactory import newdev_db_config as db_config


class DataBaseFactory:

    def __init__(self):
        print('数据库初始化。。。。。')
        configs = db_config
        print(configs.ssh_hostname)
        private_key = paramiko.RSAKey.from_private_key_file(configs.private_key)
        # 连接database
        server = SSHTunnelForwarder(
            # 指定ssh登录的跳转机的address
            ssh_address_or_host=(configs.ssh_hostname, configs.ssh_port),
            # 设置密钥
            ssh_pkey=private_key,
            # 如果是通过密码访问，可以把下面注释打开，将密钥注释即可。
            # ssh_password = "password"
            # 设置用户
            ssh_username=configs.ssh_username,
            # 设置数据库服务地址及端口
            remote_bind_address=(configs.local_hostname, configs.local_port))
        server.start()
        try:
            print("数据库建立连接。。。。。。")
            self.condb = pymysql.connect(database=configs.database,
                                         user=configs.user,
                                         password=configs.password,
                                         host=server.local_bind_host,
                                         # 因为上面没有设置 local_bind_address,所以这里必须是127.0.0.1,如果设置了，取设置的值就行了。
                                         port=server.local_bind_port)  # 这里端口也一样，上面的server可以设置，没设置取这个就行了
            print("success")
        except Exception as abnormal:
            print("数据库连接错误，错误内容%s " % abnormal)
        self.cursor = self.condb.cursor()

    def querysql(self, sql):
        try:
            num = self.cursor.execute(sql)  # 影响的行数
        except Exception as abnormal:
            print("SQL有误，错误内容 %s" % abnormal)

        if num == 0:  # 0 则代表没有查询结果
            return "没有查询的结果.."
        elif num == 1:  # 影响行数 为1 fetchone
            return list(self.cursor.fetchone())
        else:  # 多行情况下 使用fetchall
            return list(self.cursor.fetchall())


    def get_column_name(self):
        return [i[0] for i in self.cursor.description]


    def commit(self):
        self.condb.commit()


    def __del__(self):
        self.cursor.close()
        self.condb.close()
