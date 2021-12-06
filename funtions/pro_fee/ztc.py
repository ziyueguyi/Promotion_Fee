# -*- coding:utf-8 -*-
# @文件名称  :ztc
# @项目名称  :Promotion_Fee.py
# @软件名称  :PyCharm
# @创建时间  : 2021-10-19 14:24
# @用户名称  :DELL
import math
import os
import time
import urllib
import zipfile
import tempfile

import pandas as pd
from datetime import datetime
from base_fun import funtion
from base_fun.funtion import getOneday
from funtions.pub_methods import pub_method


# from tm_login import TbkDeal


class get_ztc(object):
    def __init__(self):
        self.pm = pub_method()
        self.get_contents = self.pm.get_contents
        self.get_content = self.pm.get_content
        self.columns = [
            '商品id', '花费', '店铺', '编码', '日期', '创建时间', '总花费', '推广类型', '店铺类型', '花费汇总', '直接成交金额', '投入产出比'
        ]
        self.columns_en = ['goods_id', 'money', 'shop_name', 'goods_no', 'exp_date', 'update_time',
                           'sum_cost', 'sku_type', 'shop_type', 'cost_money', 'total_amount', 'ROI']
        self.headers = {
            'origin': 'https://subway.simba.taobao.com',
            'referer': 'https://subway.simba.taobao.com/index.jsp',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'Connection': 'close',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }

    def online_download(self, fn, token, cookie, sessionID, s_date):
        url = "https://subway.simba.taobao.com/reportdownload/addMultiTask.htm"
        datas = {
            'fileName': fn,
            'dimension': '[103]',
            'startDate': s_date,
            'endDate': s_date,
            'transactionCycle': '-1',
            'aggregationMode': '1',
            'sla': 'json',
            'isAjaxRequest': 'true',
            'token': token,
            '_referer': '/report/bpreport/index',
            'sessionId': sessionID,
        }
        self.get_content(url=url, data=datas, cookie=cookie)

    @staticmethod
    def get_cookie(sn):
        cookie = './tool/tm_data/{0}/cookies/ztc_cookies.json'.format(sn)
        cookie = funtion.load_cookie(cookie)
        return cookie

    def get_token(self, cookie):
        url = 'https://subway.simba.taobao.com/bpenv/getLoginUserInfo.htm'
        response = self.get_content(url, None, cookie)
        token = response['result']['token']
        return token

    def get_cost_money(self, token, cookie, sessionId, s_date):
        queryParam = {"startDate": "{0}".format(s_date), "endDate": "{0}".format(s_date), "effect": -1}
        get_money_url = 'https://subway.simba.taobao.com/openapi/param2/1/gateway.subway/rpt/rptCampaignByDay$'
        datas = {
            'queryParam': str(queryParam),
            'sla': 'json',
            'isAjaxRequest': 'true',
            'token': token,
            '_referer': '/report/bpreport/index',
            'sessionId': sessionId
        }
        response = self.get_content(url=get_money_url, data=datas, cookie=cookie)
        try:
            value = float(response['result'][0]['costInYuan'])
        except BaseException as e:
            ''.format(e)
            value = -1
        return value

    def get_other_info(self, token, cookie, sessionId, file_name):
        url = "https://subway.simba.taobao.com/reportdownload/getdownloadTasks.htm"
        page = 0
        custId = 0
        taskId = 0
        datas = {
            'pageSize': '200',
            'pageNumber': page,
            'sla': 'json',
            'isAjaxRequest': 'true',
            'token': token,
            '_referer': '/report/bpreport/download-list',
            'sessionId': sessionId,
        }
        flag = True
        while flag:
            try:
                response = self.get_content(url=url, data=datas, cookie=cookie)
                result = response['result']
                pages = int(result['totalItem']) // 200
                if page > pages:
                    flag = False
                page += 1
                fil_list_info = result['items']
                for fli in fil_list_info[::-1]:
                    if fli['fileName'] == file_name + '_单元':
                        custId = fli['custId']
                        taskId = fli['id']
                        flag = False
                        break
            except BaseException as e:
                ''.format(e)
                flag = False
            time.sleep(1)
        return custId, taskId

    def dow_file(self, token, cookie, custId, taskId, br):
        br = funtion.route_join(br, '直通车')
        down_rul = 'https://download-subway.simba.taobao.com/download.do?spm=a2e2i.23211836.ce272de26.d5325113b.6f3d68f8Q4CscJ&custId={0}&token={1}&taskId={2}'.format(
            custId, token, taskId)
        try:
            response = self.get_contents(url=down_rul, cookie=cookie)
        except BaseException as e:
            ''.format(e)
            return

        __temp_file = tempfile.TemporaryFile()
        __temp_file.write(response)
        zf = zipfile.ZipFile(__temp_file, mode='r')

        for names in zf.namelist():
            try:
                fn = names.encode('cp437').decode('gbk')
            except BaseException as e:
                ''.format(e)
                fn = names.encode('utf-8').decode('utf-8')
            zf.extract(names, br)
            fn = fn.strip('_单元.csv') + '.csv'
            if names != fn:
                names = funtion.route_join(br, names)
                if os.path.exists(names):
                    fn = funtion.route_join(br, fn)
                    if os.path.exists(fn):
                        os.remove(fn)
                    os.rename(names, fn)
                else:
                    return False
            return True

    def del_file(self, token, cookie, task_id, sessionId):
        """删除文件"""
        url = 'https://subway.simba.taobao.com/reportdownload/deltask.htm'
        item = {
            "taskId": task_id,
            "sla": "json",
            "isAjaxRequest": "true",
            "token": token,
            "_referer": '/report/bpreport/download',
            "sessionId": sessionId,
        }
        data_encode = urllib.parse.urlencode(item)
        self.get_content(url, data_encode, cookie)

    def r_csv(self, br, fn, sn):
        fn += '.csv'
        fn = funtion.route_join(br, fn)
        datas = pd.read_csv(fn)
        datas['花费'].dropna(inplace=True, axis=0)
        sum_cost = datas['花费'].sum()
        datas['店铺'] = sn
        datas['日期'] = getOneday(1)
        datas['总花费'] = sum_cost
        datas['花费汇总'] = sum_cost
        datas['推广类型'] = 'kc'
        datas['店铺类型'] = '天猫商城'
        datas['创建时间'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        datas['编码'] = datas['商品id'].apply(lambda x: self.pm.get_sku_code(x))
        datas = datas[self.columns]
        datas.rename(columns=dict(zip(self.columns, self.columns_en)))
        datas.to_excel('./1.xlsx')
        return datas

    def run(self, sn, s_date, br, fn):
        fn = fn.strip('.csv')
        cookie = self.get_cookie(sn)
        token = self.get_token(cookie)
        sessionId = '0A0A18A1'
        cost_money = self.get_cost_money(token, cookie, sessionId, s_date)
        if cost_money not in (0, -1):
            nums, taskId, custId = (0, 0, 0)
            while nums < 3:
                self.online_download(fn, token, cookie, sessionId, s_date)
                custId, taskId = self.get_other_info(token, cookie, sessionId, fn)
                if taskId != '':
                    break
                nums += 1
                time.sleep(30)
            nums = 0
            while nums < 3 and taskId:
                flag_bool = self.dow_file(token, cookie, custId, taskId, br=br)
                if flag_bool:
                    break
                nums += 1
                time.sleep(30)
            if taskId:
                self.del_file(token, cookie, taskId, sessionId)
        return cost_money

# if __name__ == '__main__':
#     base_route = './tool'
#     td = TbkDeal()
#     dt = '2021-12-05'
#     user_name = '能良数码官方旗舰店:数据专用'
#     password = 'sjzy123456'
#     shop_name = '能良数码官方旗舰店'
#     flag_type = 2
#     file_name = '{0}{1}'.format(shop_name, dt, dt)
#     td.get_shop_cookies(user_name, password, shop_name, dt, flag_type)
#     gz = get_ztc()
#     gz.run(shop_name, dt, base_route, file_name)
