# -*- coding:utf-8 -*-
# @文件名称  :pub_methods
# @项目名称  :Promotion_Fee.py
# @软件名称  :PyCharm
# @创建时间  : 2021-10-19 14:27
# @用户名称  :DELL
import asyncio
import random

import pandas as pd
import requests
from pyppeteer import launch
from retrying import retry
from sqlalchemy import create_engine
from base_fun import funtion
from base_fun.mso import connect_info


class pub_method:
    def __init__(self):
        self.connect_pro = create_engine(connect_info())
        self.sql_sku = 'select outer_id from goods_skus_info where num_iid ="{0}"'
        self.headers = {
            'Connection': 'close',
            'origin': 'https://subway.simba.taobao.com',
            'referer': 'https://subway.simba.taobao.com/index.jsp',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8'
        }

    @retry(stop_max_attempt_number=3)
    def get_contents(self, url, cookie, data=None):
        response = requests.get(url, data=data, headers=self.headers, cookies=cookie, stream=True)
        return response.content

    @retry(stop_max_attempt_number=3)
    def get_content(self, url, data, cookie):
        response = requests.post(url, data=data, headers=self.headers, cookies=cookie)
        return response.json()

    def get_sku_code(self, good_id):
        """
        sku编码
        :param good_id: sku的id
        :return: 编码
        """
        try:
            df = pd.read_sql_query(self.sql_sku.format(good_id), self.connect_pro)
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

    @staticmethod
    def create_file(base_route, br_file, file_name):
        fn = funtion.route_join(base_route, br_file)
        funtion.chect_dir(fn)
        return funtion.route_join(fn, '{0}'.format(file_name))

    @staticmethod
    async def screen_size():
        """使用tkinter获取屏幕大小"""
        import tkinter
        tk = tkinter.Tk()
        width = tk.winfo_screenwidth()
        height = tk.winfo_screenheight()
        tk.quit()
        return width, height

    async def tb_main(self,base_url,file_route):
        funtion.chect_dir(file_route)
        width, height = await self.screen_size()
        browser = await launch(headless=False,
                               handleSIGINT=False,
                               handleSIGTERM=False,
                               handleSIGHUP=False,
                               userDataDir=file_route,
                               args=['--disable-infobars',
                                     '--no-sandbox',
                                     '--disable-setuid-sandbox',
                                     '--window-size={0},{1}'.format(width, height)],
                               dumpio=True)
        page = await browser.newPage()
        try:
            await page.setViewport({'width': width, 'height': height})
            await page.setJavaScriptEnabled(enabled=True)
            await page.goto(base_url, {'timeout': 10000 * 20, 'waitUntil': 'networkidle0'})
            await page.evaluateOnNewDocument(
                '''() =>{ Object.defineProperties(navigator, { webdriver: { get: () => false } }) }''')
            await page.evaluate('''() =>{ Object.defineProperties(navigator,{ webdriver:{ get: () => false } }) }''')
            await asyncio.sleep(random.randint(3,6))
        except BaseException as e:
            print('{0}访问失败《{1}》'.format(base_url,e))
            await page.close()
            await browser.close()
            exit()
        finally:
            return page,browser