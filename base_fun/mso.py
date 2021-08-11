# -*- coding:utf-8 -*-
# @文件名称  :ceshi.py
# @项目名称  :Promotion_Fee
# @软件名称  :PyCharm
# @创建时间  :2021-04-06 10:54
# @用户名称  :紫月孤忆
import pymysql
from base_fun.utils import get_mysql_msg


def r_sql_opt():
    params = get_mysql_msg('prod')
    config_pro = {
        'host': params['DB_HOST'],
        'user': params['DB_USER'],
        'password': params['DB_PASS'],
        'database': params['DATABASE'],
        'charset': 'utf8mb4',
        'port': params['DB_PORT']
    }
    return config_pro


def connect_info():
    r_sql_opt()
    return 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset={charset}'.format(**r_sql_opt())


def bing_mysql(str_sql, db=None, db_type="dev", tip=True):
    """
    数据库操作
    :param db:
    :param str_sql:数据库操作语句
    :param db_type: 数据库类型
    :param tip: 是否提示
    :return:
    """
    config_pro = dict()
    if db_type == "pro":
        params = get_mysql_msg('prod')
        config_pro = {
            'host': params['DB_HOST'],
            'user': params['DB_USER'],
            'password': params['DB_PASS'],
            'database': params['DATABASE'],
            'charset': 'utf8mb4',
            'port': params['DB_PORT']
        }
    elif db_type == 'dev':
        config_pro = {
            'host': 'rm-uf6h2s8g5xg6482r75o.mysql.rds.aliyuncs.com',
            'user': 'opera_man_prod',
            'password': 'nengliang2019@',
            'database': db,
            'charset': 'utf8mb4',
            'port': 3306
        }
    elif db_type == 'test':
        config_pro = {
            'host': '47.103.114.251',
            'user': 'root',
            'password': 'nengliang2019@',
            'database': 'operating-management',
            'charset': 'utf8mb4',
            'port': 3306
        }
    db = pymysql.connect(**config_pro)
    cursor = db.cursor()
    num = None
    if tip:
        # 是否产生提示，默认是提示的
        s_keyword = {
            'delete': '删除',
            'update': '更新',
            'insert': '插入'
        }
        first_world = str_sql.split(' ', 1)[0].lower()
        if first_world in s_keyword.keys():
            print('正在执行{0}命令：{1}'.format(s_keyword[first_world], str_sql))
            if input('请输入123：') != '123':
                print('数据为修改，成功退出')
                exit()
    try:
        num = cursor.execute(str_sql)
        if 'select' in str_sql.lower():
            num = cursor.fetchall()
        db.commit()
    except BaseException as e:
        print('数据错误：', e)
        db.rollback()
    finally:
        db.close()
        cursor.close()
    return num


class DC:
    def __init__(self, db=None, db_type="dev", tip=True):
        self.db = db
        self.db_type = db_type
        self.tip = tip

    def bing_mysql(self, str_sql, db=None, db_type=None, tip=None):
        """
        数据库操作
        :param db:
        :param str_sql:数据库操作语句
        :param db_type: 数据库类型
        :param tip: 是否提示
        :return:
        """
        if db:
            self.db = db
        if db_type:
            self.db_type = db_type
        if tip:
            self.tip = tip
        config_pro = dict()
        if self.db_type == "pro":
            params = get_mysql_msg('prod')
            config_pro = {
                'host': params['DB_HOST'],
                'user': params['DB_USER'],
                'password': params['DB_PASS'],
                'database': params['DATABASE'],
                'charset': 'utf8mb4',
                'port': params['DB_PORT']
            }
        elif self.db_type == 'dev':
            config_pro = {
                'host': 'rm-uf6h2s8g5xg6482r75o.mysql.rds.aliyuncs.com',
                'user': 'opera_man_prod',
                'password': 'nengliang2019@',
                'database': self.db,
                'charset': 'utf8mb4',
                'port': 3306
            }
        elif self.db_type == 'test':
            config_pro = {
                'host': '47.103.114.251',
                'user': 'root',
                'password': 'nengliang2019@',
                'database': 'operating-management',
                'charset': 'utf8mb4',
                'port': 3306
            }
        dbc = pymysql.connect(**config_pro)
        cursor = dbc.cursor()
        num = None
        s_keyword = {
            'delete': '删除',
            'update': '更新',
            'insert': '插入'
        }
        if self.tip:
            # 是否产生提示，默认是提示的
            first_world = str_sql.lower()
            if True in [i in first_world for i in s_keyword.keys()]:
                print('正在执行{0}命令：{1}'.format(s_keyword[first_world], str_sql))
                if input('请输入123：') != '123':
                    print('数据为修改，成功退出')
                    exit()
        try:
            if 'select' in str_sql.lower():
                cursor.execute(str_sql)
                num = cursor.fetchall()
            else:
                for i in str_sql.split(';'):
                    if str(i).strip():
                        num = cursor.execute(i)
            dbc.commit()
        except BaseException as e:
            print('数据错误：', e)
            dbc.rollback()
        finally:
            dbc.close()
            cursor.close()
        return num
