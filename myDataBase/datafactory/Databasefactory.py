import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder



class DataBaseFactory:

    def __init__(self, db_config):
        print('数据库初始化。。。。。')
        configs = db_config
        print(configs.hostname)
        if (hasattr(configs, 'use_ssh') and configs.use_ssh == 'yes'):
            self.__ssh = 'yes'
            self.__connect_db_by_ssh(configs)
            self.__open = 'yes'
        else:
            self.__ssh = 'no'
            self.__connect_db(configs)
            self.__open = 'yes'
        self.__cursor = self.__condb.cursor()

    def __connect_db(self, db_config):
        print("数据库建立连接。。。。。。")
        configs = db_config
        # 连接database
        try:
            self.__condb = pymysql.connect(
                host=configs.hostname,
                port=configs.port,
                db=configs.database,
                user=configs.user,
                password=configs.password,
                charset=configs.charset)
        except Exception as abnormal:
            print("数据库连接错误，错误内容%s " % abnormal)

    def __connect_db_by_ssh(self, db_config):
        print("连接 ssh。。。。。。")
        configs = db_config
        private_key = paramiko.RSAKey.from_private_key_file(configs.private_key)
        # 连接database
        self.__server = SSHTunnelForwarder(
            # 指定ssh登录的跳转机的address
            ssh_address_or_host=(configs.ssh_hostname, configs.ssh_port),
            # 设置密钥
            ssh_pkey=private_key,
            # 如果是通过密码访问，可以把下面注释打开，将密钥注释即可。
            # ssh_password = "password"
            # 设置用户
            ssh_username=configs.ssh_username,
            # 设置数据库服务地址及端口
            remote_bind_address=(configs.hostname, configs.port))
        self.__server.start()
        try:
            print("数据库建立连接。。。。。。")
            self.__condb = pymysql.connect(database=configs.database,
                                         user=configs.user,
                                         password=configs.password,
                                         host=self.__server.local_bind_host,
                                         # 因为上面没有设置 local_bind_address,所以这里必须是127.0.0.1,如果设置了，取设置的值就行了。
                                         port=self.__server.local_bind_port)  # 这里端口也一样，上面的server可以设置，没设置取这个就行了
            print("success")
        except Exception as abnormal:
            print("数据库连接错误，错误内容%s " % abnormal)


    def querysql(self, sql):
        try:
            num = self.__cursor.execute(sql)  # 影响的行数
        except Exception as abnormal:
            print("SQL有误，错误内容 %s" % abnormal)

        if num == 0:  # 0 则代表没有查询结果
            return "没有查询的结果.."
        elif num == 1:  # 影响行数 为1 fetchone
            return list(self.__cursor.fetchone())
        else:  # 多行情况下 使用fetchall
            return list(self.__cursor.fetchall())

    def get_column_name(self):
        return [i[0] for i in self.__cursor.description]

    def commit(self):
        self.__condb.commit()

    def shoutdown(self):
        if self.__ssh == 'yes':
            self.__cursor.close()
            self.__condb.close()
            self.__server.close()
        self.__open = 'no';
        print("server closed")

    def __del__(self):
        if self.__open == 'yes':
            self.shoutdown()