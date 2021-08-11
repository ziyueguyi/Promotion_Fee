# -*- coding: utf-8 -*-
# @Time    : 2021/1/28 14:30
# @Author  : Paul
from datetime import date, timedelta

import pandas as pd
import pymysql
import requests
from sqlalchemy import create_engine
# from utils.utils import get_mysql_msg
from utils import get_mysql_msg
import config
"""
天猫导入一些没有匹配到sku的推广费数据
"""

params = get_mysql_msg('prod')
DB_USER = params['DB_USER']
DB_PASS = params['DB_PASS']
DB_HOST = params['DB_HOST']
DB_PORT = params['DB_PORT']
DATABASE = params['DATABASE']


class ZtcSkuLoad:
    def __init__(self):
        self.connect = create_engine(config.connect_info)
        self.connect_pro = create_engine('mysql+pymysql://opera_python:nenglianginfo2021@python@rm-uf698x9pde1ytqxe8ko.mysql.rds.aliyuncs.com:3306/operating-management?charset=utf8mb4')
        self.get_sku_sql = 'select outer_id from goods_skus_info where num_iid ="%s" and outer_id ="%s"'
        self.save_sku_sql = """insert into goods_skus_info(num_iid,outer_id,update_date,shop_name) VALUES(%s,%s,%s,%s)"""
        self.update_sku_sql = 'update goods_skus_info set outer_id="%s" where num_iid="%s" '
        self.sql_sku = 'select outer_id from goods_skus_info where num_iid ="%s"'
        self.delete_jzt_sql = 'delete from tm_ztc_sku_none where sku_code IS NOT NULL'
        self.update_jzt_sql = 'update tm_ztc_sku_none set sku_code="%s" where sku_id="%s" '
        self.get_jzt_sql = 'select sku_id,sku_cost,sku_code,shop_type,shop_name,create_date,sku_type from %s '
        self.config_pro = {
            'host': DB_HOST,
            'user': DB_USER,
            'password': DB_PASS,
            'database': DATABASE,
            'charset': 'utf8mb4',
            'port': DB_PORT
        }

    def get_data(self):
        df = pd.read_excel('./tool/天猫推广费.xlsx',usecols=['sku_id','sku_code'])
        df.drop_duplicates('sku_id',inplace=True)
        df.dropna(subset=['sku_code'], inplace=True)

        self.update_sku(df)
        self.update_sku_pro(df)

    def update_sku(self,df):
        """sku编码空值处理"""
        conn = pymysql.connect(**self.config_pro)  # 有中文要存入数据库的话要加charset='utf8'
        cursor = conn.cursor()  # 创建游标

        for index, row in df.iterrows():
            sku_id = row['sku_id']
            sku_code = row['sku_code']
            try:
                cursor.execute(self.update_jzt_sql % (sku_code,sku_id))
                conn.commit()
            except Exception as e:
                print(e)
                conn.rollback()

        cursor.close()
        conn.close()

    def update_sku_pro(self,df):
        conn = pymysql.connect(**self.config_pro)  # 有中文要存入数据库的话要加charset='utf8'
        cursor = conn.cursor()  # 创建游标

        create_date = date.today().strftime('%Y-%m-%d')

        for index, row in df.iterrows():
            sku_id = row['sku_id']
            sku_code = row['sku_code']
            shop_name = None
            try:
                cursor.execute(self.get_sku_sql % (sku_id,sku_code))
                results = cursor.fetchall()
                if results:
                    cursor.execute(self.update_sku_sql % (sku_code,sku_id))
                    conn.commit()
                else:
                    # 插入数据
                    cursor.execute(self.save_sku_sql,(sku_id,sku_code,create_date,shop_name))
                    conn.commit()
            except Exception as e:
                conn.rollback()

        cursor.close()
        conn.close()

    def push_data(self,db,check_date):
        df = pd.read_sql(self.get_jzt_sql % db, self.connect_pro)
        df.dropna(inplace=True)
        print(df)
        # df['sku_code'] = df.sku_id.apply(self.get_sku_code)
        is_null = df.isnull().sum().sum()
        print(is_null)

        if is_null == 0:
            df.rename(columns={'sku_id': 'goods_id', 'sku_cost': 'money','create_date':'exp_date','sku_code':'goods_no'}, inplace=True)
            df.to_sql(name='om_market_t_tro_sku_gen', con=self.connect,if_exists='append',index=False)
            df.to_sql(name='om_market_t_tro_sku_gen', con=self.connect_pro,if_exists='append',index=False)

            try:
                datas = pd.read_sql(self.delete_jzt_sql, self.connect)
            except Exception as e:
                print(e)
        else:
            print('sku编码存在空值...')

    def start(self):
        yesterday = (date.today() + timedelta(days=-1)).strftime("%Y-%m-%d")  # 昨天日期

        # 空编码处理后入库
        self.get_data()

        # 处理完入库
        db = 'tm_ztc_sku_none'
        self.push_data(db, yesterday)


if __name__ == '__main__':
    ztc = ZtcSkuLoad()
    ztc.start()
