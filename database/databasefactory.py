import config
import pymysql


class DataBaseFactory:

    def __init__(self):
        print('数据库初始化。。。。。')
        configs = config.configs.get('db')
        # 连接database
        try:
            print("数据库建立连接。。。。。。")
            self.condb = pymysql.connect(
                host=configs.get("hostname"),
                port=configs.get("port"),
                db=configs.get("database"),
                user=configs.get("user"),
                password=configs.get("password"),
                charset='utf8')
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
