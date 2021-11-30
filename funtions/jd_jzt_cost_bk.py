# -*- coding:utf-8 -*-
# @文件名称  :mian
# @项目名称  :数据项目
# @软件名称  :PyCharm
# @创建时间  :2021-05-06 14:04
# @用户名称  :紫月孤忆
import os
import json
import time
import pymysql
import zipfile
import requests
import tempfile
import numpy as np
import pandas as pd
from retrying import retry
from base_fun import mso
from datetime import timedelta, datetime
from colorama import Fore, Style

"""
京东精准通费用获取,添加当天备份功能，可用于查询id出自哪个店铺
"""


class JztCost(object):
    def __init__(self):
        np.seterr(invalid='ignore')
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
            'referer': 'https://jzt.jd.com/haitou',
            'Content-Type': 'application/json;charset=UTF-8',
        }
        self.sql_sku = 'select outerId from jd_sku_code where skuId ="{0}"'
        self.get_sku_sql = 'select * from %s where sku_id ="%s" and create_date="%s" and sku_type="%s" and shop_name="%s"'
        self.save_sql = """insert into {} (sku_id,sku_cost,sku_code,create_date,create_time,sum_cost,sku_type,shop_type,shop_name,cost_money,user_name) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        self.get_cost_sql = 'select * from jd_ztc_sum where name ="%s" and yesterday_date="%s" and shop_name="%s"'
        self.save_sql_sum = """insert into jd_ztc_sum(name,yesterday_date,cost,create_time,shop_name) VALUES(%s,%s,%s,%s,%s)"""

    @staticmethod
    def cookies_load(shop_names):
        """
        处理cookies
        :param shop_names:
        :return:
        """
        cookies_dict = dict()
        with open('./tool/datas/{0}/cookies/jd_{0}.json'.format(shop_names), 'r', encoding='utf-8') as f:
            listCookies = json.loads(f.read())
        for cookie in listCookies:
            cookies_dict[cookie['name']] = cookie['value']
        return cookies_dict

    @retry(stop_max_attempt_number=3)
    def get_content(self, url, data, shop_name):
        response = requests.post(url, json=data, headers=self.headers, cookies=self.cookies_load(shop_name))
        return response.content.decode()

    @retry(stop_max_attempt_number=3)
    def get_kill_cost(self, url, time_day, shop_names):
        """
        获取请求
        :param url:
        :param time_day:
        :param shop_names:
        :return:
        """
        data = {"requestFrom": 0, "page": 1, "pageSize": 100, "startDay": time_day, "endDay": time_day,
                "platform": "", "clickOrOrderDay": 0, "clickOrOrderCaliber": 1, "giftFlag": 0,
                "orderStatusCategory": "",
                "isDaily": "false", "filters": [], "obys": "impressions|desc", "campaignId": ""}
        response = requests.post(url, json=data, headers=self.headers, cookies=self.cookies_load(shop_names))
        json_response = response.json()
        response_data = json_response.get('data')
        data_list = response_data.get('datas')
        sum_cost = response_data['ext']['cost']

        sec_kill_cost = None
        for i in data_list:
            campaign_name = i.get('campaignName')
            if campaign_name:
                if campaign_name == '秒杀计划':
                    sec_kill_cost = float(i['cost'])

        sec_kill_cost = sec_kill_cost if sec_kill_cost else 0.00
        return sec_kill_cost, sum_cost

    def add_kc_file_name(self, startDay, endDay, shop_name):
        """
        添加快车前一天要生成的文件
        :param startDay:
        :param endDay:
        :param shop_name:
        :return:
        """
        url = 'https://jzt-api.jd.com/kuaiche/download/manager/ad'
        data = {"page": 1, "pageSize": 10, "platform": None, "status": None, "filters": [], "obys": "",
                "startDay": startDay,
                "endDay": endDay,
                "campaignType": None, "putType": None, "clickOrOrderDay": 15, "clickOrOrderCaliber": 0, "giftFlag": 0,
                "orderStatusCategory": "",
                "campaignId": "", "groupId": "", "deliveryType": 0, "requestFrom": 0
                }
        reportName = "快车推广管理_推广创意报表_{}_{}".format(startDay, endDay)
        data.update({"reportName": reportName})
        self.get_content(url, data, shop_name)
        return reportName

    def add_ht_list(self, shop_name, yesterday_time, db_name, user_name, end_day, sc):
        """
        获取海投商品list
        :param sc:
        :param shop_name:
        :param yesterday_time:
        :param db_name:
        :param user_name:
        :param end_day:
        :return:
        """
        url = 'https://jzt-api.jd.com/ht/normal/campaign/list'
        data_1 = {"requestFrom": 0}
        response = self.get_content(url, data_1, shop_name)
        json_data = json.loads(response)
        ids = [i.get('id') for i in json_data['data']]
        url = 'https://jzt-api.jd.com/reweb/normal/material/campaign/list'
        data = {"startDay": yesterday_time, "endDay": yesterday_time, "platform": "all", "clickOrOrderDay": 0,
                "clickOrOrderCaliber": 1, "orderStatusCategory": None, "reqType": 0, "pageSize": 10, "pageIndex": 1,
                "giftFlag": 0,
                "ids": ids, "requestFrom": 0}
        response = self.get_content(url, data, shop_name)
        datas = json.loads(response)['data']['datas']
        sku_list = []
        # cost = 0
        for i in datas:
            cost = i['cost']
            if cost != '0.00':
                page_id = i['id']
                url = 'https://jzt-api.jd.com/reweb/normal/material/brand/list'
                data_2 = {"campaignType": 3, "startDay": yesterday_time, "endDay": yesterday_time, "id": page_id,
                          "groupId": str(int(page_id) + 1), "platform": "all", "clickOrOrderDay": 0,
                          "clickOrOrderCaliber": 1,
                          "orderStatusCategory": None, "val": "", "filters": [], "obys": "impressions|desc",
                          "reqType": 0,
                          "page": 1, "pageSize": 100, "giftFlag": 0, "requestFrom": 0}
                response = self.get_content(url, data_2, shop_name)
                detail_data = json.loads(response)
                brand_list = detail_data['data']['datas']
                for item in brand_list:
                    time.sleep(5)
                    sum_cost = item['cost'][0]
                    if float(sum_cost) >= float('0.01'):
                        name = item['name']
                        skuBrandId = item['skuBrandId']
                        skuCid3 = item['skuCid3']
                        # page_index = 1

                        # while True:
                        #     json_sku = self.page_data(yesterday_time, page_id, name, skuBrandId, skuCid3, shop_name, page_index)
                        #     pages_sku = json_sku['data']['paginator']['pages']
                        #     data_list = json_sku['data']['datas']
                        #     if page_index > pages_sku + 1:
                        #         break
                        #     for j in data_list:
                        #         item = {'sku_id': j['id'], 'sku_cost': float(j['cost'][0])}
                        #         sku_list.append(item)
                        #     page_index += 1

                        json_sku = self.page_data(yesterday_time, page_id, name, skuBrandId, skuCid3, shop_name, 1)
                        pages_sku = json_sku['data']['paginator']['pages']
                        data_list = json_sku['data']['datas']
                        for dl in data_list:
                            item = {'sku_id': dl['id'], 'sku_cost': float(dl['cost'][0])}
                            sku_list.append(item)
                        for ps in range(2, pages_sku + 1):
                            json_skus = self.page_data(yesterday_time, page_id, name, skuBrandId, skuCid3, shop_name,
                                                       ps)
                            data_lists = json_skus['data']['datas']
                            for j in data_lists:
                                # print(name, j['id'], j['cost'][0])
                                item = {'sku_id': j['id'], 'sku_cost': float(j['cost'][0])}
                                sku_list.append(item)

        try:
            df = pd.DataFrame(sku_list)
            if df.shape[0]:
                df = df.groupby(by='sku_id').sum().reset_index()[['sku_id', 'sku_cost']]
                df['sku_id'] = df['sku_id'].astype(str)
                self.save_file('ht', shop_name, end_day, df)
                df['sku_code'] = df.sku_id.apply(self.get_sku_code)
                df = df[(df['sku_cost'] != 0.00) | (df['sku_cost'] != 0)].copy()
                df['create_date'] = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
                df['create_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sum_cost = np.around(df.sku_cost.sum(), 2)
                diff_cost = float(sc) - sum_cost
                df_num = df.shape[0]
                df['sku_cost'] = df.sku_cost + round(diff_cost / df_num if df_num > 0 else 1, 2)
                df['sku_cost'] = np.around(df['sku_cost'], 2)
                # cz = float(float(cost) - df.sku_cost.sum())
                # try:
                #     if cz > 0:
                #         print(cz/df.shape[0])
                #         print(df['sku_cost'])
                #         df['sku_cost'] -= cz/df.shape[0]
                # except BaseException as e:
                #     print('{0}({1}):'.format(shop_name, db_name, e),
                #           Fore.LIGHTRED_EX + '部分费用无法分摊' + Style.RESET_ALL, cz)
                df['sku_cost'] = df['sku_cost'].round(2)
                df['sum_cost'] = np.around(df.sku_cost.sum(), 2)
                df['cost_money'] = sc
                df['user_name'] = user_name
                df['shop_name'] = shop_name
                df['shop_type'] = '京东商城'
                df['sku_type'] = db_name
                df4 = df[df.sku_code.isna()]  # None
                df5 = df[df.sku_code.notna()]
                # 处理空值
                self.save_date(df4, 'jd_ztc_cost_none')
                # # 处理正常值入库
                self.save_date(df5, 'jd_ztc_cost')
            else:
                print('{0}:{1}'.format(shop_name, db_name), Fore.LIGHTRED_EX + '没有订单存在' + Style.RESET_ALL)
        except Exception as e:
            print(shop_name, e, len(sku_list), sku_list)

    def page_data(self, yesterday_time, pf_id, name, skuBrandId, skuCid3, shop_name, page):
        sku_data = {"campaignType": 3, "startDay": yesterday_time, "endDay": yesterday_time,
                    "groupId": str(int(pf_id) + 1),
                    "platform": "all", "clickOrOrderDay": 0, "clickOrOrderCaliber": 1, "orderStatusCategory": None,
                    "val": "", "filters": [], "obys": "impressions|desc", "reqType": 0, "page": page, "pageSize": 100,
                    "giftFlag": 0, "skuBrandId": skuBrandId, "skuCid3": skuCid3, "campaignId": str(pf_id),
                    "proName": name, "requestFrom": 0}
        url = "https://jzt-api.jd.com/reweb/normal/material/product/list"
        time.sleep(5)
        response = self.get_content(url, sku_data, shop_name)
        json_sku = json.loads(response)
        return json_sku

    def get_sku_code(self, good_id):
        """
        sku编码
        :param good_id: sku的id
        :return: 编码
        """
        try:
            df = pd.read_sql_query(self.sql_sku.format(good_id), mso.connect_info())
            data = df.to_dict('records')[0]
            sku_code = data.get('outerId')
            sku_code = None if ';' in sku_code else sku_code
            if sku_code:
                if len(sku_code) >= 18:
                    return sku_code[:18]
                else:
                    return None
            else:
                return None
        except IndexError:
            return None

    def add_file_name(self, startDay, endDay, shop_name):
        """添加前一天要生成的文件"""
        url = 'https://jzt-api.jd.com/touchpoint/download/ad'
        data = {"page": 1, "pageSize": 10, "platform": "", "status": "", "filters": [], "obys": "",
                "startDay": startDay, "endDay": endDay,
                "clickOrOrderDay": 15, "clickOrOrderCaliber": 0, "giftFlag": 0, "orderStatusCategory": None,
                "campaignId": "", "groupId": "", "campaignType": "",
                "putType": "", "billingType": None, "requestFrom": 0}
        reportName = "购物触点推广管理_推广创意报表_{0}_{1}".format(startDay, endDay)
        data.update({"reportName": reportName})
        self.get_content(url, data, shop_name)
        return reportName

    def get_file(self, reportName, data, cost_money, db_name, shop_name, name, end_day):
        """
        获取生产文件列表
        :param reportName:
        :param data:
        :param cost_money:
        :param db_name:
        :param shop_name:
        :param name:
        :param end_day:
        :return:
        """
        time.sleep(60)
        url = 'https://jzt-api.jd.com/api/download/common/asyn/download/reportInfo/list'
        response = self.get_content(url, data, shop_name)
        response_json = json.loads(response)
        datas = response_json['data']['datas']
        for i in datas:
            if i['status'] == 2 and i['reportName'] == reportName:
                reportName = i['reportName']
                url = i['downloadUrl']
                self.down_file(url, reportName, cost_money, db_name, shop_name, name, end_day)
                # 删除文件
                time.sleep(2)
                delete_url = 'https://jzt-api.jd.com/api/download/common/asyn/download/reportInfo/batch/delete'
                datas = {"ids": [i['id']], "requestFrom": 0}
                self.get_content(delete_url, datas, shop_name)
                if db_name == "kc":
                    break

    @staticmethod
    def get_route(str_data):
        ntCtime_dt = datetime.strptime(str_data, "%Y-%m-%d")  # str转datetime.datetime类型
        ntCtime = datetime.date(ntCtime_dt)
        return str(ntCtime.year), str(ntCtime.month), str(ntCtime.day)

    def save_file(self, act_type, shop_name, end_day, data):
        if 'kc' in act_type:
            ad = '快车'
        elif 'cd' in act_type:
            ad = '触点'
        else:
            ad = '海投'
        mnd = self.get_route(end_day)
        file_route = os.path.join('./tool/', mnd[0], mnd[1], mnd[2], ad).replace('\\', '/')
        if not os.path.exists(file_route):
            try:
                os.makedirs(file_route)
            except FileExistsError as e:
                print(2, e)
        file_route = os.path.join(file_route, shop_name + '.xlsx').replace('\\', '/')
        try:
            data.to_excel(file_route, index_label=None, index=None)
        except Exception as e:
            print(3, e)

    def down_file(self, url, name, cost_money, db_name, shop_name, user_name, end_day):
        response = requests.get(url)
        route_fn = './tool/datas/{0}/cache/'.format(shop_name)
        if not os.path.exists(route_fn):
            os.makedirs(route_fn)
        try:
            _tmp_file = tempfile.TemporaryFile()  # 创建临时文件
            _tmp_file.write(response.content)  # byte字节数据写入临时文件
            zf = zipfile.ZipFile(_tmp_file, mode='r')
            for names in zf.namelist():
                zf.extract(names, route_fn)
            file_name = route_fn + name + '_0.csv'
        except zipfile.BadZipFile:
            file_name = route_fn + name + '.csv'
            with open(file_name, 'w', encoding='utf-8') as f:
                f.write(response.content.decode())
        self.deal_data(file_name, name, end_day, cost_money, user_name, db_name, shop_name)

    def deal_data(self, file_name, name, end_day, cost_money, user_name, db_name, shop_name):
        if os.path.exists(file_name):
            df = pd.read_csv(file_name)
            self.save_file('kc' if '快车' in name else 'cd', shop_name, end_day, df)
            df.rename(columns={'商品SKU': 'sku_id', '推广计划': 'promotion_plan', '总费用': 'sku_cost'},
                      inplace=True)
            df.rename(columns={'定向方式': 'sum_type'} if '定向方式' in df.keys() else {'资源位': 'sum_type'}, inplace=True)
            df1 = df[df.sum_type == '汇总']
            dfs = df1.groupby('sku_id').sum().reset_index()[['sku_id', 'sku_cost']]
            df3 = dfs[(dfs.sku_cost != 0.00) | (dfs.sku_cost != 0)].copy()
            df3['sku_code'] = df3.sku_id.apply(self.get_sku_code)  # 获取商品sku
            df3['create_date'] = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
            df3['create_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sum_cost = np.around(df3.sku_cost.sum(), 2)
            order_num = df3.shape[0]
            diff_cost = round((float(cost_money) - sum_cost) / order_num if order_num else 1, 2)
            df3['sku_cost'] = df3['sku_cost'] + diff_cost
            less_df = float(float(cost_money) - df3.sku_cost.sum())
            try:
                if less_df > 0.0 and df3.shape[0]:
                    df3.loc[df3.shape[0] - 1, 'sku_cost'] += round(less_df, 2)
            except Exception as e:
                print('{0}({1}):'.format(shop_name, db_name, e), Fore.LIGHTRED_EX + '部分费用无法分摊' + Style.RESET_ALL)
            df3['sku_cost'] = np.around(df3.sku_cost, 2)
            df3['sum_cost'] = np.around(df3.sku_cost.sum(), 2)
            df3['cost_money'] = cost_money
            df3['user_name'] = user_name
            df3['shop_name'] = shop_name
            df3['shop_type'] = '京东商城'
            df3['sku_type'] = db_name
            df_dict = {
                'jd_ztc_cost': df3[df3.sku_code.notna()],
                'jd_ztc_cost_none': df3[df3.sku_code.isna()],

            }
            for k, v in df_dict.items():
                self.save_date(v, k)
            # 删除文件
            os.remove(file_name)

    def save_date(self, df, db_name):
        """
        数据入库
        :param df:
        :param db_name:
        :return:
        """
        conn = pymysql.connect(**mso.r_sql_opt())  # 有中文要存入数据库的话要加charset='utf8'
        cursor = conn.cursor()  # 创建游标
        yes_date = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
        for index, row in df.iterrows():
            sku_id = int(row['sku_id'])
            sku_cost = row['sku_cost']
            sku_code = row['sku_code']
            create_date = row['create_date']
            create_time = row['create_time']
            sum_cost = row['sum_cost']
            sku_type = row['sku_type']
            shop_type = row['shop_type']
            shop_name = row['shop_name']
            cost_money = row['cost_money']
            user_name = row['user_name']
            try:
                cursor.execute(self.get_sku_sql % (db_name, sku_id, yes_date, sku_type, shop_name))
                results = cursor.fetchall()
                if results:
                    pass
                else:
                    cursor.execute(self.save_sql.format(db_name), (
                        sku_id, sku_cost, sku_code, create_date, create_time, sum_cost, sku_type, shop_type, shop_name,
                        cost_money, user_name))
                    conn.commit()
            except Exception as e:
                print(6, e)
                conn.rollback()
        cursor.close()
        conn.close()

    def cost_yesterday(self, name, business_type, start_day, end_day, shop_name):
        """
        前一天花费
        :param name:
        :param business_type:
        :param start_day:
        :param end_day:
        :param shop_name:
        :return:
        """
        url = 'https://jzt-api.jd.com/common/indicator?requestFrom=0'
        data = {"businessType": business_type, "granularity": 4, "startDay": start_day, "endDay": start_day,
                'clickOrOrderCaliber': 0, 'clickOrOrderDay': 15,'orderStatusCategory': 'null'}
        response = requests.post(url, json=data, headers=self.headers, cookies=self.cookies_load(shop_name))
        json_response = response.json()
        try:
            cost = json_response['data']['cost']['main']
        except Exception as e:
            print(shop_name, 'login 过期', e)
            cost = 0

        item = {'name': name, 'yesterday_date': start_day, 'cost': cost,
                'create_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'shop_name': shop_name}
        return item

    def get_cost(self, start_day, end_day, shop_name):
        data_list = []
        r_dict = dict()
        pf_type = {'京东汇总': -1, '京东快车': 2, '京东触点': 16777216, '京东海投': 524288, '京东展位': 1, '京东直投': -2}
        for key, value in pf_type.items():
            item = self.cost_yesterday(key, value, start_day, end_day, shop_name)
            data_list.append(item)
            r_dict[item['name']] = item['cost']
        df = pd.DataFrame(data_list)
        self.save_cost(df)
        return r_dict

    def save_cost(self, df):
        """
        首页数据入库
        :param df:
        :return:
        """
        conn = pymysql.connect(**mso.r_sql_opt())  # 有中文要存入数据库的话要加charset='utf8'
        cursor = conn.cursor()  # 创建游标
        for index, row in df.iterrows():
            name = row['name']
            yesterday_date = row['yesterday_date']
            cost = row['cost']
            create_time = row['create_time']
            shop_name = row['shop_name']
            try:
                cursor.execute(self.get_cost_sql % (name, yesterday_date, shop_name))
                results = cursor.fetchall()
                if results:
                    pass
                else:
                    cursor.execute(self.save_sql_sum, (name, yesterday_date, cost, create_time, shop_name))
                    conn.commit()
            except Exception as e:
                print(8, e)
                conn.rollback()
        cursor.close()
        conn.close()

    def start_one(self, start_day, end_day, shop_name, name):
        """
        多线程运行
        :param start_day:
        :param end_day:
        :param shop_name:
        :param name:
        :return:
        """
        # 获取总计金额
        sum_cost = self.get_cost(start_day, end_day, shop_name)
        pfd_list = [shop_name, name]
        sn = Fore.LIGHTGREEN_EX + '{0}'.format(shop_name) + Style.RESET_ALL
        try:
            if abs(int(sum_cost['京东汇总'])) != 0:
                # 快车
                sku_type_kc = '京东快车'
                if abs(int(sum_cost[sku_type_kc])) != 0:
                    try:
                        reportName = self.add_kc_file_name(start_day, end_day, shop_name)
                        data = {"page": 1, "pageSize": 10, "type": 1, "requestFrom": 0}
                        self.get_file(reportName, data, sum_cost[sku_type_kc], 'kc', shop_name, name, end_day)
                        sku_type = Fore.LIGHTBLUE_EX + sku_type_kc + Style.RESET_ALL
                    except BaseException as e:
                        print(e)
                        # print(Fore.LIGHTRED_EX + '{0}:{1}获取失败'.format(sn, sku_type_kc, e) + Style.RESET_ALL)
                        sku_type = Fore.LIGHTRED_EX + sku_type_kc + '获取失败' + Style.RESET_ALL
                    finally:
                        pass
                else:
                    sku_type = Fore.LIGHTRED_EX + sku_type_kc + Style.RESET_ALL
                print('{0}({1}):{2}'.format(sn, sku_type, sum_cost[sku_type_kc]))

                # 触点
                sku_type_cd = '京东触点'
                if abs(int(sum_cost[sku_type_cd])) != 0:
                    try:
                        report_name = self.add_file_name(start_day, end_day, shop_name)
                        data_cd = {"page": 1, "pageSize": 10, "type": 3, "requestFrom": 0}
                        self.get_file(report_name, data_cd, sum_cost[sku_type_cd], 'tp', shop_name, name, end_day)
                        sku_type = Fore.LIGHTBLUE_EX + sku_type_cd + Style.RESET_ALL
                    except BaseException as e:
                        # print(Fore.LIGHTRED_EX + '{0}:{1}获取失败'.format(sn, sku_type_cd, e) + Style.RESET_ALL)
                        sku_type = Fore.LIGHTRED_EX + sku_type_cd + '获取失败' + Style.RESET_ALL
                    finally:
                        pass
                else:
                    sku_type = Fore.LIGHTRED_EX + sku_type_cd + Style.RESET_ALL
                print('{0}({1}):{2}'.format(sn, sku_type, sum_cost[sku_type_cd]))

                # 海投
                sku_type_ht = '京东海投'
                if abs(int(sum_cost[sku_type_ht])) != 0:
                    try:
                        self.add_ht_list(shop_name, start_day, 'ht', name, end_day, sum_cost[sku_type_ht])
                        sku_type = Fore.LIGHTBLUE_EX + sku_type_ht + Style.RESET_ALL
                    except BaseException as e:
                        ''.format(e)
                        sku_type = Fore.LIGHTRED_EX + sku_type_ht + '获取失败' + Style.RESET_ALL
                        # print(Fore.LIGHTRED_EX + '{0}:{1}获取失败'.format(sn, sku_type_ht, e) + Style.RESET_ALL)
                    finally:
                        pass
                else:
                    sku_type = Fore.LIGHTRED_EX + sku_type_ht + Style.RESET_ALL
                print('{0}({1}):{2}'.format(sn, sku_type, sum_cost[sku_type_ht]))
                r_amount = round(
                    sum_cost['京东汇总'] - sum_cost[sku_type_kc] - sum_cost[sku_type_cd] - sum_cost[sku_type_ht],
                    2)
                sku_type_qt = '其他推广'
                if r_amount:
                    sku_type = Fore.LIGHTRED_EX + sku_type_qt + Style.RESET_ALL
                else:
                    sku_type = Fore.LIGHTBLUE_EX + sku_type_qt + Style.RESET_ALL
                print('{0}({1}):{2}'.format(sn, sku_type, r_amount))
                pfd_list.extend(
                    [float(sum_cost[sku_type_kc]), float(sum_cost[sku_type_ht]), float(sum_cost[sku_type_cd]),
                     r_amount])
            else:
                pfd_list.extend([None, None, None, None])
                print('{0}:'.format(sn), Fore.LIGHTYELLOW_EX + '没有花费推广费' + Style.RESET_ALL)
        except BaseException as e:
            print(e)
        finally:
            return pfd_list

    def start(self, username, shopname):
        time_start = datetime.now()
        end_day = time_start.strftime('%Y-%m-%d')
        # date_yesterday = datetime.now() + timedelta(days=-1)
        start_day = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
        return self.start_one(start_day, end_day, shopname, username)
