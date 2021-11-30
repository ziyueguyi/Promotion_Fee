# -*- coding:utf-8 -*-
# @文件名称  :ztc
# @项目名称  :Promotion_Fee.py
# @软件名称  :PyCharm
# @创建时间  : 2021-10-19 14:24
# @用户名称  :DELL
import os
import time
import urllib
import zipfile
import tempfile
from base_fun import funtion
from jsonpath import jsonpath
from colorama import Fore, Style
from funtions.pub_methods import pub_method


class get_ztc(object):
    def __init__(self):
        pm = pub_method()
        self.get_contents = pm.get_contents
        self.get_content = pm.get_content

    def ztc_content(self, yesterday_time, shop_name, base_route):
        base_route = funtion.route_join(base_route, '直通车')
        funtion.chect_dir(base_route)
        cookie = './tool/tm_data/{0}/cookies/ztc_cookies.json'.format(shop_name)
        cookie = funtion.load_cookie(cookie)
        token = self.get_token(cookie)
        sum_cost = self.get_cost(token, yesterday_time, cookie)
        # 直通车
        fn = '{0}{1}'.format(shop_name, yesterday_time)
        file_id = self.get_ztc_cost(token, yesterday_time, cookie, fn)
        if file_id:
            time.sleep(30)
            nums = 0
            while nums < 3:
                nums += 1
                time.sleep(30)
                custId = self.ztc_online_download(token, cookie)
                if not custId:
                    continue
                self.down_file(cookie, base_route, custId, file_id)
                file_names = funtion.route_join(base_route, '{0}.csv'.format(fn))
                if os.path.exists(file_names):
                    break
                print(Fore.LIGHTGREEN_EX + "{0}:第{1}尝试".format(shop_name, nums) + Style.RESET_ALL)
            self.delete_file(token, file_id, cookie)
        return sum_cost

    def get_token(self, cookie):
        url = 'https://subway.simba.taobao.com/bpenv/getLoginUserInfo.htm'
        response = self.get_content(url, None, cookie)
        token = response['result']['token']
        return token

    def get_cost(self, token, yesterday_time, cookie):
        """
        获取直通车花费总额
        :param token:
        :param yesterday_time:
        :param cookie:
        :return:
        """
        url = f'https://subway.simba.taobao.com/report/rptBpp4pCustomSum.htm?startDate={yesterday_time}&endDate={yesterday_time}&effect=-1'
        refer = f'/report/bpreport/index?start={yesterday_time}&end={yesterday_time}&page=1'
        data = {
            'sla': 'json',
            'isAjaxRequest': 'true',
            'token': token,
            '_referer': refer,
            'sessionId': '3742c665-a4f6-45ab-8270-74aab81b6940'
        }
        data_encode = urllib.parse.urlencode(data)
        response = self.get_content(url, data_encode, cookie)
        code = response['code']
        if code == '200':
            try:
                sum_cost = float(jsonpath(response, '$..cost')[0]) / 100
            except TypeError:
                sum_cost = 0
            return sum_cost
        else:
            return None

    def get_ztc_cost(self, token, yesterday_time, cookie, file_name):
        """
        添加快车前一天要生成的文件
        :param token:
        :param yesterday_time:
        :param cookie:
        :param file_name:
        :return:
        """
        url = 'https://subway.simba.taobao.com/reportdownload/addtask.htm'
        refer = f'/report/bpreport/index?start={yesterday_time}&end={yesterday_time}&page=1'
        item = {
            "fileName": file_name,
            "dimension": 2,
            "startDate": yesterday_time,
            "endDate": yesterday_time,
            "sla": "json",
            "isAjaxRequest": "true",
            "token": token,
            "_referer": refer,
            "sessionId": "2d87e4f4-e086-41dd-93bd-e0fc81eb03b5"
        }
        data_encode = urllib.parse.urlencode(item)
        response = self.get_content(url, data_encode, cookie)
        code = response['code']
        if code == '200':
            return response['result']
        else:
            return None

    def down_file(self, cookie, base_route, cost_id, file_id):
        """
        把文件下载带本地
        :param cookie: 获取的cookie，进行验证
        :param base_route: 文件的保存路径
        :param cost_id: 获取的验证编码
        :param file_id: 获取的文件编码
        :return: None
        """
        url = f'https://download-subway.simba.taobao.com/download.do?spm=a2e2i.11816827.0.0.10c868f8695S29&custId={cost_id}&taskId={file_id}&token=abc77a1a'
        response_content = self.get_contents(url, cookie)
        _tmp_file = tempfile.TemporaryFile()  # 创建临时文件
        _tmp_file.write(response_content)  # byte字节数据写入临时文件
        zf = zipfile.ZipFile(_tmp_file, mode='r')
        for names in zf.namelist():
            try:
                file_names = names.encode('cp437').decode('gbk')
            except BaseException as e:
                ''.format(e)
                file_names = names.encode('utf-8').decode('utf-8')
            zf.extract(names, base_route)
            if names != file_names:
                names = funtion.route_join(base_route, names)
                if os.path.exists(names):
                    file_names = funtion.route_join(base_route, file_names)
                    if os.path.exists(file_names):
                        os.remove(file_names)
                    os.rename(names, file_names)

    def delete_file(self, token, task_id, cookie):
        """
        删除文件
        :param token:
        :param task_id:
        :param cookie:
        :return:
        """
        url = 'https://subway.simba.taobao.com/reportdownload/deltask.htm'
        item = {
            "taskId": task_id,
            "sla": "json",
            "isAjaxRequest": "true",
            "token": token,
            "_referer": '/report/bpreport/download',
            "sessionId": "845378f3-7c8f-41e4-8448-c74f4be9c797",
        }
        data_encode = urllib.parse.urlencode(item)
        self.get_content(url, data_encode, cookie)

    def ztc_online_download(self, token, cookie):
        """
        在线文件下载并获取在线文件数据
        :param token:
        :param cookie:
        :return:
        """
        url = 'https://subway.simba.taobao.com/reportdownload/getdownloadTasks.htm?pageSize=100&pageNumber=1'
        item = {
            "sla": "json",
            "isAjaxRequest": "true",
            "token": token,
            "_referer": '/report/bpreport/download',
            "sessionId": "d5450ae8-7332-4dc7-8160-18247caad3c8",
        }
        data_encode = urllib.parse.urlencode(item)
        response = self.get_content(url, data_encode, cookie)
        custId = 0
        if response['code']:
            custId = response['result']['items'][0]['custId']
        return custId
