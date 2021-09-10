# -*- coding:utf-8 -*-
# @文件名称  :月度费用统计
# @项目名称  :Promotion_Fee.py
# @软件名称  :PyCharm
# @创建时间  : 2021-09-05 11:41
# @用户名称  :DELL
import pandas
import os

import pandas as pd

from base_fun.mso import DC
from base_fun.funtion import route_join
from datetime import datetime, date, timedelta


class Monthly_expense_statistics(object):
    def __init__(self):
        self.dc = DC(db_type='pro')
        self.shop_type = {
            'jd_sku_code': ['skuId', 'outerId'],
            'goods_skus_info': ['num_iid', 'outer_id'],
            '快车': ['商品SKU', '总费用', '资源位'],
            '海投': ['sku_id', 'sku_cost'],
            '淘宝客': ['商品ID', '佣金', '服务费金额'],
            '直通车': ['商品id', '花费(分)'],
            '触点': ['商品SKU', '总费用', '资源位'],
            '超级推荐': ['宝贝id', '消耗'],
        }
        self.columns = ['shop_name', 'sku_type', 'good_id', 'money', 'data']
        self.goods_info = dict()

    def read_datas(self, file_route, sku_type):
        if '.csv' in file_route:
            datas = pd.read_csv(file_route)
        else:
            datas = pd.read_excel(file_route)
        st = self.shop_type[sku_type]
        datas.dropna(inplace=True, axis=1)
        if datas.shape[0]:
            column = st[0:2]
            if sku_type == '淘宝客':
                datas = datas[st]
            else:
                if len(st) != 2:
                    datas = datas[datas[st[-1]] == '汇总']
                datas = datas[column]
            datas = datas[datas[st[1]] != 0]
            if sku_type in ['直通车']:
                datas[st[1]] = datas[st[1]] / 100
            column_dict = dict(zip(column, self.columns[2:4]))
            datas.rename(columns=column_dict, inplace=True)
        else:
            datas = pd.DataFrame(columns=['good_no', 'money', ])
        return datas

    def read_route(self, route):
        datas = pd.DataFrame(self.columns)
        if os.path.exists(route):
            first_route_lists = os.listdir(route)
            for f_frl in first_route_lists:
                dt = f_frl
                dt_new = datetime.now()
                month = route[-1]
                dt = "{0}-{1}-{2}".format(dt_new.year, month, dt)
                f_frl = route_join(route, f_frl)
                if os.path.exists(f_frl):
                    second_frl = os.listdir(f_frl)
                    for s_frl in second_frl:
                        s_frls = route_join(f_frl, s_frl)
                        if os.path.exists(s_frls):
                            three_frl = os.listdir(s_frls)
                            for t_frl in three_frl:
                                shop_name = t_frl.split('.')[0]
                                t_frl = route_join(s_frls, t_frl)
                                if '补差文档' not in t_frl:
                                    data = self.read_datas(t_frl, s_frl)
                                    if data.shape[0]:
                                        data['data'] = dt
                                        data['sku_type'] = s_frl
                                        data['shop_name'] = shop_name
                                        datas = datas.append(data, ignore_index=True)
        return datas

    def get_sql(self, data):
        if data['good_id'] not in self.goods_info.keys():
            if data['sku_type'] in ['快车', '海投', '触点']:
                db = 'jd_sku_code'
            else:
                db = 'goods_skus_info'
            s_str = "select {1} from {2} where {0} = '{3}'".format(*self.shop_type[db], db, data['good_id'])
            datas = self.dc.bing_mysql(s_str)
            print(datas)
            if datas:
                self.goods_info[data['good_id']] = datas[0][0]
            else:
                self.goods_info[data['good_id']] = None
        return self.goods_info[data['good_id']]

    def run(self):
        base_route = './tool/2021/8'
        datas = self.read_route(base_route)
        if datas.shape[0]:
            datas['good_id'] = datas['good_id'].astype(str)
            datas['good_no'] = datas.apply(lambda x: self.get_sql(x), axis=1)
            datas.to_excel('1.xlsx')


if __name__ == '__main__':
    mes = Monthly_expense_statistics()
    mes.run()
