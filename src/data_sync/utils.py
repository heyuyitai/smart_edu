import pymysql
from pymysql.cursors import DictCursor

from src.config import config


class MySqlReader:
    def __init__(self):
        self.conn=pymysql.connect(**config.MYSQL_CONFIG)
        self.cursor=self.conn.cursor(DictCursor)

    def read(self):
        sql="""
        select * from favor_info
        """
        self.cursor.execute(sql)
        res=self.cursor.fetchall()
        print(res)


if __name__=="__main__":
    sql_reader=MySqlReader()

    sql_reader.read()