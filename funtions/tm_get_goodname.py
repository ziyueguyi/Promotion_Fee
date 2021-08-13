# -*- coding:utf-8 -*-
# @文件名称  :main
# @项目名称  :数据项目
# @软件名称  :PyCharm
# @创建时间  :2021-06-22 9:29
# @用户名称  :紫月孤忆
import asyncio
import random

import pandas as pd
import win32api
import win32con
from pyppeteer import launch

from base_fun.mso import DC


class g_sn:
    def __init__(self):
        self.dc = DC(db_type='pro', tip=False)
        self.base_url = 'https://detail.tmall.com/item.htm?id='

    async def main(self):
        good_list = dict()
        width = win32api.GetSystemMetrics(win32con.SM_CXSCREEN)
        height = win32api.GetSystemMetrics(win32con.SM_CYSCREEN)
        browser = await launch({
            'headless': False,
            'userDataDir': './tool/tm_data/userdata',
            'handleSIGINT': False,
            'handleSIGTERM': False,
            'handleSIGHUP': False,
            'dumpio': True,
            'args': ['--disable-infobars',
                     '--no-sandbox',
                     '--window-size={0},{1}'.format(width, height)]
        })
        page = await browser.newPage()
        try:
            await page.setUserAgent(
                'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36')
            await page.setViewport({'width': width, 'height': height})
            page.setDefaultNavigationTimeout(1000 * 20)
            data = await self.g_good_id()
            if data:
                for i in data:
                    s_sql = None
                    try:
                        print(self.base_url + str(i))
                        await page.goto(self.base_url + str(i),
                                        {'timeout': 1000 * 20})
                        await asyncio.sleep(random.randint(2, 4))
                        await page.evaluateOnNewDocument(
                            '''() =>{ Object.defineProperties(navigator, { webdriver: { get: () => false } }) }''')
                        good_name = await page.Jx('//*[@id="J_DetailMeta"]/div[1]/div[1]/div/div[1]/h1/a')
                        if not good_name:
                            good_name = await page.Jx('//*[@id="J_DetailMeta"]/div[1]/div[1]/div/div[1]/h1')
                            if not good_name:
                                good_name = await page.Jx('//*[@id="content"]/div[1]/div/h2')
                        good_name = str(await (await good_name[0].getProperty('textContent')).jsonValue()).strip()
                        good_list[str(i)] = good_name
                        if '很抱歉' in good_name or '补差价' in good_name:
                            s_sql = "DELETE FROM tm_ztc_sku_none WHERE sku_id='{0}'".format(i)
                        else:
                            s_sql = "UPDATE tm_ztc_sku_none SET goods_name = '{0}' WHERE sku_id = '{1}'".format(
                                good_name, i)
                    except BaseException as e:
                        print(e)
                    finally:
                        self.dc.bing_mysql(s_sql)
                    # time.sleep(random.randint(2, 6))
                    await page.waitFor(random.randint(2, 6))
        except BaseException as e:
            print(e)
        finally:
            await page.waitFor(2000)
            await browser.close()

    async def g_good_id(self):
        s_sql = "SELECT DISTINCT sku_id FROM `tm_ztc_sku_none` WHERE goods_name IS NULL and (sku_code IS NULL OR sku_code='')"
        data = self.dc.bing_mysql(s_sql)
        if data:
            data = [i[0] for i in data]
            return data
        else:
            return None

    def get_data(self, filename):
        s_sql = "SELECT sku_id,sku_cost,sku_code,sku_type,shop_name,create_time,goods_name FROM `tm_ztc_sku_none` WHERE sku_code IS NULL OR sku_code=''"
        data = self.dc.bing_mysql(s_sql)
        if data:
            data = pd.DataFrame(data, columns=['商品编号', '花费', '商品sku', '推广费类型', '店铺名称', '日期', '商品名称'])
            data.to_excel(filename, index_label=False, index=False)

    def tgg_run(self, filename):
        asyncio.get_event_loop().run_until_complete(self.main())
        self.get_data(filename)


if __name__ == '__main__':
    gs = g_sn()
    gs.tgg_run('123.xlsx')
