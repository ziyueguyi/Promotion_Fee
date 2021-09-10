# -*- coding:utf-8 -*-
# @文件名称  :tm_login.py
# @项目名称  :Promotion_Fee
# @软件名称  :PyCharm
# @创建时间  :2021-07-26 10:54
# @用户名称  :紫月孤忆
import os
import time
import urllib
import random
import pymysql
import asyncio
import zipfile
import requests
import tempfile
import numpy as np
import pandas as pd
from retrying import retry
from pyppeteer import launch
from jsonpath import jsonpath
from colorama import Fore, Style
from base_fun import funtion, mso, send_message
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
from configparser import ConfigParser
from funtions.tm_get_goodname import g_sn

"""
天猫淘宝客账号登陆
"""


class TbkDeal:
    def __init__(self):
        self.headers = {
            'origin': 'https://subway.simba.taobao.com',
            'referer': 'https://subway.simba.taobao.com/index.jsp',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }
        self.connect_pro = create_engine(
            'mysql+pymysql://opera_python:nenglianginfo2021@python@rm-uf698x9pde1ytqxe8ko.mysql.rds.aliyuncs.com:3306/operating-management?charset=utf8mb4')
        self.sql_sku = 'select outer_id from goods_skus_info where num_iid ="%s"'
        self.get_sku_sql = 'select * from %s where create_date="%s" and  sku_id ="%s" and sku_type="%s" and shop_name="%s"'
        self.save_sql = """insert into {0} {1} VALUES{2}"""

    def get_sku_code(self, good_id):
        """
        sku编码
        :param good_id: sku的id
        :return: 编码
        """
        try:
            df = pd.read_sql_query(self.sql_sku % good_id, self.connect_pro)
            df.dropna(subset=['outer_id'], inplace=True)
            data = df.to_dict('records')[0]
            sku_code = data.get('outer_id')
            if sku_code:
                if len(sku_code) >= 18:
                    return sku_code[:18]
                else:
                    return None
            else:
                return None
        except Exception as e:
            ''.format(e)
            return None

    def deal_tbk(self, file_path: str, shop_name: str, cost_money: float, cols: list, *args) -> pd.DataFrame:
        if os.path.exists(file_path):
            ''.format(args)
            df = pd.read_csv(file_path)
            df.rename(columns={'商品ID': 'goods_id', '佣金': 'money', '服务费金额': 'server_cost'}, inplace=True)
            df['money'] = df['money'] + df['server_cost']
            df1 = df.groupby('goods_id').sum().reset_index()[['goods_id', 'money']]
            df1 = df1[df1.money != 0]
        else:
            df1 = pd.DataFrame(columns=cols)
        return self.data_format(df1, cost_money, shop_name, cols, 'tbk')

    def save_data(self, db_name, dt, conn, cursor, yes_date):
        try:
            cursor.execute(self.get_sku_sql % (db_name, yes_date, *list(dt[['sku_id', 'sku_type', 'shop_name']])))
            results = cursor.fetchall()
            if results:
                pass
            else:
                i_str = self.save_sql.format(db_name, str(tuple(dt.keys())).replace("'", ""),
                                             str(tuple([i for i in dt])))
                cursor.execute(i_str)
                conn.commit()
        except Exception as e:
            print(e, db_name)
            conn.rollback()

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

    def start(self, shop_name, cols, time_start, time_end, flag_type=0, flag=True):
        ''.format(time_end)

        start_day = time_start - timedelta(hours=time_start.hour, minutes=time_start.minute,
                                           seconds=time_start.second, microseconds=time_start.microsecond)
        date_yesterday = time_start + timedelta(days=-1)
        zeroToday = date_yesterday - timedelta(hours=date_yesterday.hour, minutes=date_yesterday.minute,
                                               seconds=date_yesterday.second, microseconds=date_yesterday.microsecond)
        yesterday_time = date_yesterday.strftime('%Y-%m-%d')

        d_year, d_month, d_day = funtion.get_route(start_day.strftime('%Y-%m-%d'))
        base_route = funtion.route_join('./tool', d_year, d_month, d_day)
        funtion.chect_dir(base_route)
        file_route_dict = dict()
        if flag_type == 0 or flag_type == 1:
            sum_cost = 0
            if flag:
                sum_cost = self.tbk_content(zeroToday, shop_name, base_route)
            file_route_dict['淘宝客'] = [self.deal_tbk, self.create_file(base_route, '淘宝客', '{0}.csv'.format(shop_name)),
                                      sum_cost]
        if flag_type == 0 or flag_type == 2:
            sum_cost = 0
            if flag:
                sum_cost = self.ztc_content(yesterday_time, shop_name, base_route)
            file_route_dict['直通车'] = [self.deal_ztc, self.create_file(base_route, '直通车',
                                                                      '{0}{1}.csv'.format(shop_name, yesterday_time)),
                                      sum_cost]
        if flag_type == 0 or flag_type == 3:
            yt = yesterday_time
            file_route_dict['超级推荐'] = [self.deal_sr, self.create_file(base_route, '超级推荐', '{0}.xlsx'.format(shop_name)),
                                       yt]

        new_pd = pd.DataFrame(columns=cols)
        for k, i in file_route_dict.items():
            try:
                pdd = i[0](i[1], shop_name, i[2], cols)
            except BaseException as e:
                ''.format(e)
                status = Fore.RED + "数据获取失败"
            else:
                if pdd.shape[0]:
                    new_pd = new_pd.append(pdd)
                    status = Fore.BLUE + '数据获取成功'
                else:
                    status = Fore.YELLOW + '数据获取为空'
            print('{0}:{1}》{2}'.format(shop_name, k, status), end='')
            print(Style.RESET_ALL)
        return new_pd

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

    @staticmethod
    def create_file(base_route, br_file, file_name):
        fn = funtion.route_join(base_route, br_file)
        funtion.chect_dir(fn)
        return funtion.route_join(fn, '{0}'.format(file_name))

    @retry(stop_max_attempt_number=3)
    def get_content(self, url, data, cookie):
        response = requests.post(url, data=data, headers=self.headers, cookies=cookie)
        return response.json()

    @retry(stop_max_attempt_number=3)
    def get_contents(self, url, cookie, data=None):
        response = requests.get(url, data=data, headers=self.headers, cookies=cookie, stream=True)
        return response.content

    def get_token(self, cookie):
        url = 'https://subway.simba.taobao.com/bpenv/getLoginUserInfo.htm'
        response = self.get_content(url, None, cookie)
        token = response['result']['token']
        return token

    def get_cost(self, token, yesterday_time, cookie):
        """获取总额"""
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
        """添加快车前一天要生成的文件"""
        url = 'https://subway.simba.taobao.com/reportdownload/addtask.htm'
        refer = f'/report/bpreport/index?start={yesterday_time}&end={yesterday_time}&page=1'
        # file_name = f'天猫直通车报表{yesterday_time}'

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
        """删除文件"""
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

    def deal_ztc(self, file_path: str, shop_name: str, cost_money: float, cols: list, *args):
        if os.path.exists(file_path):
            ''.format(args)
            df = pd.read_csv(file_path)
            df.rename(columns={'商品id': 'goods_id', '花费(分)': 'money', '总成交金额(分)': 'total_amount', '投入产出比': 'ROI'},
                      inplace=True)
            df1 = df.groupby('goods_id').sum().reset_index()[['goods_id', 'money', 'total_amount', 'ROI']]
            df1 = df1[df1.money != 0]
        else:
            df1 = pd.DataFrame(columns=cols)
        return self.data_format(df1, cost_money, shop_name, cols, 'ztc', mul=100,
                                total_amount=np.around(df1.total_amount, 2) / 100, ROI=np.around(df1.ROI, 2))

    @staticmethod
    def data_format(df1, cost_money, shop_name, cols, sku_type, mul=1, total_amount=0, ROI=0):
        new_pd = pd.DataFrame(columns=cols)
        if df1.size:
            df1['money'] = np.around(df1.money, 2) / mul
            df1['shop_name'] = shop_name
            df1['sum_cost'] = cost_money
            df1['sku_type'] = sku_type
            df1['total_amount'] = total_amount
            df1['ROI'] = ROI
            df1 = df1[cols]
            new_pd = new_pd.append(df1)
            return new_pd
        else:
            return new_pd

    def deal_sr(self, file_path: str, shop_name: str, yesterday_time, cols: list, *args):
        """超级推荐数据处理"""
        if os.path.exists(file_path):
            ''.format(args)
            df = pd.read_excel(file_path)
            df.rename(columns={'宝贝id': 'goods_id', '消耗': 'money', '日期': 'date_time'}, inplace=True)
            df.set_index('date_time', inplace=True)
            df = df.sort_index()
            df = df.loc[yesterday_time:yesterday_time]
            df1 = df.groupby('goods_id').sum().reset_index()[['goods_id', 'money']]
            df1 = df1[df1.money != 0]
            cost_money = np.around(df1.money.sum(), 2)
        else:
            df1 = pd.DataFrame(columns=cols)
            cost_money = 0
        return self.data_format(df1, cost_money, shop_name, cols, 'reco')

    @staticmethod
    async def screen_size():
        """使用tkinter获取屏幕大小"""
        import tkinter
        tk = tkinter.Tk()
        width = tk.winfo_screenwidth()
        height = tk.winfo_screenheight()
        tk.quit()
        return width, height

    async def tb_main(self, user_name, password, shop_name, today_dt, flag_type):
        file_route = './tool/tm_data/{0}'.format(shop_name)
        funtion.chect_dir(file_route)
        width, height = await self.screen_size()
        base_url = 'https://www.alimama.com/member/login.htm'
        browser = await launch(headless=False, handleSIGINT=False, handleSIGTERM=False, handleSIGHUP=False,
                               userDataDir=funtion.route_join(file_route, 'userdata'),

                               args=['--disable-infobars',
                                     '--no-sandbox',
                                     '--disable-setuid-sandbox''--window-size={0},{1}'.format(width, height)],
                               dumpio=True)
        page = await browser.newPage()
        try:
            await page.setViewport({'width': width, 'height': height})
            await page.setJavaScriptEnabled(enabled=True)
            await page.goto(base_url, {'timeout': 10000 * 20, 'waitUntil': 'networkidle0'})
            await page.evaluateOnNewDocument(
                '''() =>{ Object.defineProperties(navigator, { webdriver: { get: () => false } }) }''')
            await page.evaluate('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
            frame = page.frames[1]  # num为所需要的frame在所有iframe中的编号
            await frame.evaluate('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')

            await frame.type('#fm-login-id', user_name, {'delay': self.input_time_random() - 50})
            await frame.type('#fm-login-password', password, {'delay': self.input_time_random()})
            await frame.click('.fm-btn')
            await asyncio.sleep(5)
            show_url = 'https://www.alimama.com/index.htm'
            if page.url != show_url:
                await asyncio.sleep(60)
            fr = funtion.route_join(file_route, 'cookies')
            funtion.chect_dir(fr)
            tm_hd_dict = dict()
            if flag_type == 0 or flag_type == 1:
                tm_hd_dict['tbk'] = {
                    'url': 'https://ad.alimama.com/myunion.htm',
                    'cookie_name': 'tbk_cookies.json',
                    'but_id': None
                }
            if flag_type == 0 or flag_type == 2:
                tm_hd_dict['ztc'] = {
                    'url': 'https://subway.simba.taobao.com/index.jsp',
                    'cookie_name': 'ztc_cookies.json',
                    'but_id': '//*[@id="mx_190"]/div/div[2]/div[2]/a'
                }
            if flag_type == 0 or flag_type == 3:
                tm_hd_dict['cjzz'] = {
                    'url': 'https://tuijian.taobao.com/indexbp.html#!/report/whole/index?alias=all&perspective=report',
                    'cookie_name': 'cjzz_cookies.json',
                    'but_id': '//*[@id="mx_171"]/div/div[2]/div[2]/a'
                }

            for K, v in tm_hd_dict.items():
                await self.get_cookies(page, fr, v, show_url)

            if flag_type == 0 or flag_type == 3:
                xpath_url = await (
                    await (await page.xpath('//*[@id="xp_main_app"]/div[4]/div/div/a'))[0].getProperty(
                        'href')).jsonValue()  # 获取下载按钮上面的下载码链接
                xpath_url = str(xpath_url)
                if xpath_url:
                    bu = 'https://tuijian.taobao.com'
                    if bu not in xpath_url:
                        xpath_url = 'https://tuijian.taobao.com' + xpath_url
                    self.down_excel(xpath_url, fr, shop_name, today_dt)
                else:
                    print('获取链接error...')
        except Exception as e:
            print('main error:', e)
        finally:
            await page.close()
            await browser.close()

    @staticmethod
    def input_time_random():
        return random.randint(100, 151)

    @staticmethod
    def down_excel(url, fr, sn, today_dt):
        headers = {
            'Connection': 'close',
            'referer': 'https://tuijian.taobao.com/indexbp.html',
            'sec-fetch-user': '?1',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'sec-fetch-site': 'same-origin',
            'sec-fetch-mode': 'navigate',
            'accept-encoding': 'gzip, deflate, br',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4542.2 Safari/537.36',
        }
        fr = funtion.route_join(fr, 'cjzz_cookies.json')
        cookies = funtion.load_cookie(fr)
        if cookies:
            d_year, d_month, d_day = funtion.get_route(today_dt)
            file_route = funtion.route_join('./tool/', d_year, d_month, d_day, '超级推荐')
            funtion.chect_dir(file_route)
            response = requests.get(url, headers=headers, cookies=cookies, stream=True)
            file_route = funtion.route_join(file_route, '{0}.xlsx'.format(sn))
            with open(file_route, 'wb') as f:
                f.write(response.content)
        else:
            print('未获取到正确的cookies')

    @staticmethod
    async def get_cookies(page, file_route, thd, show_url):
        try:
            if page.url != show_url:
                await page.goto(show_url, {'timeout': 10000 * 20, 'waitUntil': 'networkidle0'})
                await asyncio.sleep(random.randint(2, 6))
        except BaseException as e:
            ''.format(e)
        await page.goto(thd['url'], {'timeout': 10000 * 20, 'waitUntil': 'networkidle0'})
        await asyncio.sleep(5)
        try:
            if thd['but_id']:
                await page.click(thd['but_id'], {'timeout': 10000 * 20, 'waitUntil': 'networkidle0'})
        except BaseException as e:
            ''.format(e)
        cookies = await page.cookies()
        await asyncio.sleep(random.randint(1, 5))
        fr = funtion.route_join(file_route, thd['cookie_name'])
        funtion.save_cookie(cookies, fr)
        await asyncio.sleep(random.randint(5, 15))

    def get_shop_cookies(self, user_name, password, shop_name, dt, flag_type):
        try:
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            loop = asyncio.get_event_loop()
            task = asyncio.ensure_future(self.tb_main(user_name, password, shop_name, dt, flag_type))
            loop.run_until_complete(asyncio.wait([task]))
        except Exception as e:
            print(e)

    @staticmethod
    def data_time_cal(data_range=1, start_time=None):
        """
        输入时间范围和结束最近的时间
        :param data_range:
        :param start_time:
        :return:
        """
        time_start = datetime.now()  # 获取当前的日期时间
        time_end = time_start - timedelta(data_range)
        if start_time and isinstance(time_end, datetime):
            time_start = start_time
        else:
            print("请检查输入的时间类型是否正确")

    @funtion.add_time
    def get_start(self, sn=None, flag_type=0, flag=True, data_range=1):
        time_start = datetime.now()

        time_end = time_start - timedelta(data_range)
        config = ConfigParser()
        config.read("./tm_config.ini", encoding="utf-8")
        sections = config.sections()
        cols = ['goods_id', 'money', 'shop_name', 'sku_type', 'sum_cost', 'total_amount', 'ROI']
        new_pd = pd.DataFrame(columns=cols)
        # if not sn:
        # sn = [i for i in sections]
        sn = [[i, config.get(i, 'user_name'), config.get(i, 'password')] for i in sections if not sn or i in sn]
        print([i[0] for i in sn])
        dt = time_start.strftime('%Y-%m-%d')
        for i in sn:
            # shop_name = i
            shop_name = i[0]
            user_name = i[1]
            password = i[2]
            try:
                # 数据处理
                if flag:
                    self.get_shop_cookies(user_name, password, shop_name, dt, flag_type)
                    print('{0}》'.format(shop_name) + Fore.GREEN + '登录成功', Style.RESET_ALL)
                else:
                    print(Fore.GREEN + '使用离线数据', Style.RESET_ALL)
                new_pd = new_pd.append(self.start(shop_name, cols, time_start, time_end, flag_type, flag))
            except BaseException as e:
                print(e, Fore.LIGHTRED_EX + '{0}:未获取成功'.format(shop_name), Style.RESET_ALL)
        if new_pd.shape[0]:
            self.del_data(new_pd, time_start)
        else:
            print('今天没有获取推广费')

    # def get_datas(self,data,dt,flag_type,cols,time_start,time_end):
    #     shop_name,user_name,password = *data
    #     try:
    #         # 数据处理
    #         if flag:
    #             self.get_shop_cookies(user_name, password, shop_name, dt, flag_type)
    #             print('{0}》'.format(shop_name) + Fore.GREEN + '登录成功', Style.RESET_ALL)
    #         else:
    #             print(Fore.GREEN + '使用离线数据', Style.RESET_ALL)
    #         new_pd = new_pd.append(self.start(shop_name, cols, time_start, time_end, flag_type, flag))
    #     except BaseException as e:
    #         print(e, Fore.LIGHTRED_EX + '{0}:未获取成功'.format(shop_name), Style.RESET_ALL)
    def del_data(self, new_pd, time_start):
        new_pd['shop_type'] = '天猫商城'
        new_pd['exp_date'] = (time_start + timedelta(days=-1)).strftime('%Y-%m-%d')
        new_pd['update_time'] = time_start.strftime('%Y-%m-%d %H:%M:%S')
        new_pd['goods_no'] = new_pd['goods_id'].apply(self.get_sku_code)
        new_pd['cost_money'] = new_pd['sum_cost']
        new_pd = new_pd[['goods_id', 'money', 'shop_name', 'goods_no', 'exp_date', 'update_time',
                         'sum_cost', 'sku_type', 'shop_type', 'cost_money', 'total_amount', 'ROI']]
        new_clos = ['sku_id', 'sku_cost', 'shop_name', 'sku_code', 'create_date', 'create_time', 'sum_cost', 'sku_type',
                    'shop_type', 'cost_money', 'total_amount', 'ROI']
        cols_dict = dict(zip(new_pd.columns, new_clos))
        new_pd.rename(columns=cols_dict, inplace=True)

        new_pd['sku_cost'].fillna(0, inplace=True)
        new_pd['sku_cost'] = new_pd['sku_cost'].round(2)

        h_data_pd = new_pd[new_pd.sku_code.notna()].copy(deep=True)
        n_data_pd = new_pd[new_pd.sku_code.isna()].copy(deep=True)
        h_data_pd.fillna('', inplace=True)
        n_data_pd.fillna('', inplace=True)
        conn = pymysql.connect(**mso.r_sql_opt())  # 有中文要存入数据库的话要加charset='utf8'
        cursor = conn.cursor()  # 创建游标
        yes_date = (datetime.now() + timedelta(days=-1)).strftime('%Y-%m-%d')
        print(Fore.LIGHTGREEN_EX + '数据保存开始', Style.RESET_ALL)
        if h_data_pd.shape[0]:
            h_data_pd.apply(lambda x: self.save_data('tm_ztc_sku', x, conn, cursor, yes_date), axis=1)
        else:
            print(Fore.LIGHTRED_EX + '未获取到推广费也' + Style.RESET_ALL)
        if n_data_pd.shape[0]:
            n_data_pd.apply(lambda x: self.save_data('tm_ztc_sku_none', x, conn, cursor, yes_date), axis=1)
        else:
            print(Fore.LIGHTGREEN_EX + '没有未计算的推广费' + Style.RESET_ALL)
        print(Fore.LIGHTGREEN_EX + '数据保存结束', Style.RESET_ALL)
        cursor.close()
        conn.close()

        # d_year, d_month, d_day = funtion.get_route(time_start.strftime('%Y-%m-%d'))
        # filename = funtion.route_join('./tool', d_year, d_month, d_day, '补差文档')
        # funtion.chect_dir(filename)
        # filename = funtion.route_join(filename, '天猫推广费.xlsx')
        # gn = g_sn()
        # gn.tgg_run(filename)
        # send = send_message.Send()
        # send.send_file(filename)


if __name__ == '__main__':
    print('程序启动')
    #  ft 代表运行那种推广费,0：获取所有，1:代表淘宝客，2：代表直通车，3：代表品牌新享
    #  fg 代表是否使用离线的excel报表,True 为在线文档 False使用本地的Excel
    #  dr 代表获取的日期范围

    ft = 0
    fg = True
    dr = 1
    shopname_list = []
    td = TbkDeal()
    td.get_start(sn=shopname_list, flag_type=ft, flag=fg, data_range=dr)

    scheduler = BlockingScheduler()
    scheduler.add_job(td.get_start, 'cron', hour=7, minute=10, misfire_grace_time=1000 * 90)
    scheduler.start()
