# 导入pymysql模块
import pymysql
import paramiko
from sshtunnel import SSHTunnelForwarder
import pro_db_config

configs = pro_db_config.PRO_DB_CONFIG

print(configs)

private_key = paramiko.RSAKey.from_private_key_file(configs.private_key)

with SSHTunnelForwarder(
    # 指定ssh登录的跳转机的address
    ssh_address_or_host = (configs.ssh_hostname,configs.ssh_port),
    # 设置密钥
    ssh_pkey = private_key,
    # 如果是通过密码访问，可以把下面注释打开，将密钥注释即可。
    # ssh_password = "password"
    # 设置用户
    ssh_username = configs.ssh_username,
    # 设置数据库服务地址及端口
    remote_bind_address= (configs.local_hostname,configs.local_port)) as server:

    conn = pymysql.connect(database=configs.database,
                            user=configs.user,
                            password=configs.password,
                            host=server.local_bind_host,  # 因为上面没有设置 local_bind_address,所以这里必须是127.0.0.1,如果设置了，取设置的值就行了。
                            port=server.local_bind_port) # 这里端口也一样，上面的server可以设置，没设置取这个就行了


    print(conn)

    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    sql = """
    select * from account limit 0,10
    """
    # 执行查询，查看结果，验证数据库是否链接成功。
    cur.execute(sql)

    results = cur.fetchall()

    for oneResult in results:
        print(oneResult)

    conn.close()



