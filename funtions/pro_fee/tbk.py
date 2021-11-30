# -*- coding:utf-8 -*-
# @文件名称  :tbk
# @项目名称  :Promotion_Fee.py
# @软件名称  :PyCharm
# @创建时间  : 2021-10-19 14:24
# @用户名称  :DELL
import random
import time
from datetime import timedelta, datetime

import requests
from colorama import Fore, Style
from jsonpath import jsonpath

from base_fun import funtion
from funtions.pub_methods import pub_method


class get_tbk(object):
    def __init__(self):
        pm = pub_method()
        self.headers = pm.headers
        self.create_file = pm.create_file

    def tbk_content(self, zeroToday, shop_name, base_route) -> float:
        """
        淘宝客报表获取
        :param base_route:
        :param zeroToday:
        :param shop_name:
        :return:
        """
        headers = {
            'Host': 'union-file-center.oss-cn-zhangjiakou.aliyuncs.com',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
            'Sec-Fetch-Site': 'cross-site',
            'Sec-Fetch-Mode': 'navigate',
            'Referer': 'https://ad.alimama.com/report/overview/orders.htm?spm=a21an.7676007.1998473182.dadf5a6a0.6e6e61dbysS7Ds&startTime=2021-01-23&endTime=2021-01-23&pageNo=1&jumpType=0&positionIndex=',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Sec-Fetch-User': '?1',
        }
        tbk_fr = self.create_file(base_route, '淘宝客', '{0}.csv'.format(shop_name))
        lastToday = zeroToday + timedelta(hours=23, minutes=59, seconds=59)
        file_name = './tool/tm_data/{0}/cookies/tbk_cookies.json'.format(shop_name)
        cookies = funtion.load_cookie(file_name)
        data_now = datetime.now()
        endTime = (data_now + timedelta(days=-1)).strftime('%Y-%m-%d')
        startTime = (data_now + timedelta(days=-7)).strftime('%Y-%m-%d')
        t = int(time.time())

        get_token_url = 'https://ad.alimama.com/cps/shopkeeper/loginMessage.json'  # 获取token的链接
        task_url = 'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/shopkeeper.rpt.taskstart.json'  # 添加下载在线报表的的链接
        url_id = 'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/shopkeeper.rpt.filelink.json'  # 下载报表到本地的链接
        sum_cost = None
        # 获取token
        response = self.while_task(get_token_url, headers=self.headers, cookies=cookies)
        if response:
            token = response['data']['_tb_token_']
            sum_url = f'https://ad.alimama.com/account/incomeDetail.json?t={t}&_tb_token_={token}'  # 查询具体费用的链接
            delete_url = f'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/shopkeeper.rpt.taskdel.json?t={t}&_tb_token_={token}'  # 删除报表的链接
            data = {
                't': t,
                '_tb_token_': token,
                'startTime': zeroToday,
                'endTime': lastToday,
                'bizType': 1,
                'status': 3
            }

            file_list_data = {
                't': t,
                '_tb_token_': token,
                'pageNo': 1,
                'pageSize': 10,
                'startTime': zeroToday,
                'endTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'bizType': 1,
                'status': 3
            }
            file_names = f'{zeroToday}~{lastToday}-订单结算明细报表'
            other_dict = {
                'data': file_list_data,
                'file_names': file_names
            }
            task_ret = self.while_task(task_url, data, self.headers, cookies, get_type=False, other_dict=other_dict)
            if task_ret:
                data_sum = {
                    'pageNo': 1,
                    'pageSize': 40,
                    'startTime': startTime,
                    'endTime': endTime
                }
                response_content = self.while_task(sum_url, data=data_sum, headers=self.headers, cookies=cookies)
                if response_content:
                    sum_data = response_content['data']['result']
                    for i in sum_data:
                        if i['deductDate'] == endTime:
                            sum_data = i['deductAmount']
                            break
                    if sum_data:
                        sum_cost = sum_data
                        time.sleep(60)
                        f_id = task_ret['data']['idList']
                        data = {
                            't': t,
                            '_tb_token_': token,
                            'pageNo': 1,
                            'pageSize': 10,
                            'startTime': zeroToday,
                            'endTime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'bizType': 1,
                            'idList': f_id
                        }
                        res_content = self.while_task(url_id, data=data, headers=self.headers, cookies=cookies,
                                                      get_type=False)
                        if res_content:
                            result_url = jsonpath(res_content, '$..url')[0]
                            try:
                                response_ = requests.get(result_url, headers=headers).content.decode()
                                with open(tbk_fr, 'w', encoding='utf-8', newline='') as f:
                                    f.write(response_)
                                # 删除文件
                                datas = {
                                    'bizType': '1',
                                    'ids': [f_id],
                                }
                                # 删除任务
                                if not self.while_task(delete_url, datas, self.headers, cookies):
                                    print(Fore.RED + '{0}文件删除失败'.format(shop_name))
                            except BaseException as e:
                                print(Fore.RED + '{0}:文件写入失败;{1}'.format(shop_name, e))
                        else:
                            print(Fore.RED + '{0}:文件链接获取失败'.format(shop_name))
                    else:
                        print(Fore.RED + '{0}:没有淘宝客'.format(shop_name))
                else:
                    print(Fore.RED + '{0}:消费总金额获取失败'.format(shop_name))
            else:
                print(Fore.RED + '{0}:添加文件任务失败'.format(shop_name))
        else:
            print(Fore.RED + '{0}:身份获取失败'.format(shop_name))
        print(Style.RESET_ALL, end='', sep='')
        return sum_cost

    def while_task(self, task_url, data=None, headers=None, cookies=None, fre_num=3, get_type=True, other_dict=None):
        task_num = 0
        task_ret = None
        while task_num < fre_num:
            try:
                if get_type:
                    task_ret = requests.post(task_url, data=data, headers=headers, cookies=cookies).json()
                else:
                    task_ret = requests.get(task_url, params=data, headers=headers, cookies=cookies).json()
                task_num += 1
                time.sleep(random.randint(3, 5))
                if 'bizErrorDesc' in task_ret.keys() and task_ret['bizErrorDesc'] == '重复提交任务' and other_dict:
                    f_id = self.exit_file(cookies=cookies, other_dict=other_dict)
                    if f_id:
                        task_ret['data'] = {'idList': f_id}
                        break
                if ('success' in task_ret.keys() and task_ret['success']) or ('data' in task_ret.keys() and (
                        '_tb_token_' in task_ret['data'].keys() or 'result' in task_ret['data'].keys())):
                    break
                else:
                    task_ret = None
            except BaseException as e:
                ''.format(e)
        return task_ret

    def exit_file(self, cookies, other_dict):
        """
        当淘宝客文件已存在的时候,无法重新创建
        :param cookies:
        :param other_dict:
        :return:
        """
        down_url = 'https://ad.alimama.com/openapi/param2/1/gateway.unionadv/shopkeeper.rpt.process.list.json'
        data = other_dict['data']
        file_names = other_dict['file_names']
        down_content = requests.get(down_url, params=data, headers=self.headers,
                                    cookies=cookies).json()
        for i in down_content['data']['result']:
            file_name = i['fileName']
            if file_names == file_name:
                return i['id']
        return None
