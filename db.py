import pymysql
import datetime
from config import CONFIGURATION as config


SERVER = config['SERVER']


class Db:

    def __init__(self):
        self.connection = pymysql.connect(**SERVER)


    def __exit__(self, exception_type, exception_value, traceback):
        self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        # print(sql, params)
        with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, params)
            result = cursor.fetchall()
            # print(result)
            return result
        return None

    def fetch(self, sql, params=None):
        with self.connection.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()
    
    def transaction(self):
        self.connection.begin()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()


def test_db():
    db = Db()
    sql = 'SELECT NOW() date, VERSION() version'
    return db.execute(sql)

def execute(sql, params=None):
    db = Db()
    result = db.execute(sql, params)
    db.commit()
    return result


def fetch(sql, params=None):
    db = Db()
    return db.fetch(sql, params)


