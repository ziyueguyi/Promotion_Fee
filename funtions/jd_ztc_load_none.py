# -*- coding:utf-8 -*-
# @文件名称  :jd_ztc_load_none
# @项目名称  :Promotion_Fee
# @软件名称  :PyCharm
# @创建时间  :2021-06-20 9:57
# @用户名称  :紫月孤忆
import os
import pandas as pd
from base_fun import funtion
from colorama import Fore, Style
from base_fun.mso import DC
from datetime import datetime as dt
from datetime import timedelta as td


class deal_none:
    def __init__(self, flag_type):
        self.dc = DC(db_type='pro', tip=False)
        self.base_route = '../files'
        self.flag_type = flag_type
        self.columns = ['商品编号', '商品sku', '店铺名称']
        self.type_dbs = {
            True: ['jd_sku_code', 'jd_ztc_cost', 'jd_ztc_cost_none', 'om_market_t_tro_sku_gen'],
            False: ['goods_skus_info', 'tm_ztc_sku', 'tm_ztc_sku_none', 'om_market_t_tro_sku_gen']
        }
        self.db_columns = {
            'jd_sku_code': ['skuId', 'outerId', 'shop_name', 'user_name', 'update_date'],
            'goods_skus_info': ['num_iid', 'outer_id', 'shop_name', 'user_name', 'create_time'],

            'jd_ztc_cost': ['sku_id', 'sku_cost', 'sku_code', 'shop_name', 'sku_type'],

            'tm_ztc_sku': ['sku_id', 'sku_code', 'shop_name', 'sku_type', 'shop_type', 'total_amount', 'ROI',
                           'sku_cost'],

            'jd_ztc_cost_none': ['sku_id', 'sku_code', 'sku_cost', 'shop_name', 'sku_type'],
            'tm_ztc_sku_none': ['sku_id', 'sku_code', 'sku_cost', 'shop_name', 'sku_type', 'total_amount',
                                'ROI'],
            'om_market_t_tro_sku_gen': ['goods_id', 'goods_no', 'money', 'shop_name', 'sku_type', 'total_amount', 'ROI',
                                        'exp_date', 'shop_type', 'update_time', 'initiator'],
        }
        self.zi_type = {True: '责任人', False: '商品名称'}
        self.type_db = self.type_dbs[self.flag_type]
        self.columns.append(self.zi_type[self.flag_type])

    def r_excel(self, filename):
        data = pd.DataFrame(columns=self.columns)
        if os.path.exists(filename):
            data = pd.read_excel(filename, dtype={'商品sku': str})
            try:
                data.dropna(axis=1, inplace=True)
                data = data[self.columns]
                data['商品sku'] = data['商品sku'].str.strip().apply(lambda x: x[:18])
                data['店铺名称'] = data['店铺名称'].str.strip()
                data[self.columns[3]] = data[self.columns[3]].str.strip()
                data = data[data['商品sku'].str.len() >= 18]
                # data.drop_duplicates(subset=['商品sku', '店铺名称'], keep='first', inplace=True)
            except BaseException as e:
                print(e)
            finally:
                return data
        else:
            return data

    def tra_paths(self, ):
        if self.flag_type:
            route_dir = funtion.route_join(self.base_route, 'jd_load_files')
        else:
            route_dir = funtion.route_join(self.base_route, 'tm_load_files')
        data = pd.DataFrame(columns=self.columns)
        if os.path.exists(route_dir):
            file_list = os.listdir(route_dir)
            for fl in file_list:
                data = data.append(self.r_excel(os.path.join(route_dir, fl).replace('\\', '/')))
        return data

    def tx_sku(self, data, od):
        s_sql = """SELECT COUNT(*) FROM {0} WHERE sku_id = "{1}"
                """.format(self.type_db[2], data['商品编号'])
        num = self.dc.bing_mysql(s_sql)
        if num[0][0]:
            u_sql = """UPDATE {0} SET sku_code = "{1}" WHERE sku_id="{2}" AND shop_name = "{3}";
            """.format(self.type_db[2], data['商品sku'], data['商品编号'], od)
            try:
                self.dc.bing_mysql(u_sql)
                return True
            except BaseException as e:
                print(e)
                return False
        else:
            return True

    @staticmethod
    def split_(filed):
        filed = filed
        try:
            filed = filed.split('_')[0]
        except BaseException as e:
            print(e, 3)
        finally:
            return filed

    def tj_sku(self, data):
        old_shop = data['店铺名称']
        try:
            data['店铺名称'] = self.split_(data['店铺名称'])
        except BaseException as e:
            print(e, 2)
        if self.tx_sku(data[['商品编号', '商品sku', '店铺名称']], old_shop):
            s_sql = """SELECT {0} FROM {1} WHERE {2}="{3}"
            """.format(','.join(self.db_columns[self.type_db[0]]), self.type_db[0], self.db_columns[self.type_db[0]][0],
                       data['商品编号'])
            num = self.dc.bing_mysql(s_sql)
            flag_num = 0
            for i in num:
                if data['商品sku'] == i[1]:
                    flag_num += 1
            if not self.flag_type:
                data[3] = ''
            if flag_num != 1:
                try:
                    new_data = [i for i in data]
                    new_data.append(dt.now().strftime('%Y-%m-%d %H:%M:%S'))
                    i_sql = """DELETE FROM {0} WHERE {1} = "{2}";      
                               INSERT INTO {0} ({3})VALUES {4} 
                              """.format(self.type_db[0], self.db_columns[self.type_db[0]][0], data['商品编号'],
                                         ','.join([i for i in self.db_columns[self.type_db[0]]]),
                                         str(tuple(new_data)), self.type_db[2])
                    self.dc.bing_mysql(i_sql)
                except BaseException as e:
                    print(e, 1)
            elif num[0][2] != data['店铺名称'] or num[0][3] != data[self.zi_type[self.flag_type]]:
                pic = data[self.zi_type[self.flag_type]]
                if not self.flag_type:
                    pic = num[0][3]
                u_sql = """UPDATE {0} SET shop_name="{1}" , user_name = "{2}" WHERE {3} = "{4}" AND {5} = "{6}"
                """.format(self.type_db[0], data['店铺名称'], pic, self.db_columns[self.type_db[0]][0],
                           data['商品编号'],
                           self.db_columns[self.type_db[0]][1], data['商品sku'])
                self.dc.bing_mysql(u_sql)
            else:
                print('{0}》数据已配置'.format(data['商品编号']))
        else:
            print('配置错误')

    def i_tgf(self):
        s_sql = """SELECT {0} FROM {1} having length(sku_code)=18
                """.format(','.join(self.db_columns[self.type_db[2]]), self.type_db[2])
        num = self.dc.bing_mysql(s_sql)
        if num:
            data_df = pd.DataFrame(num, columns=[self.db_columns[self.type_db[2]]])
            today_dt = dt.now()
            d_time1 = dt.strptime(str(today_dt.date()) + '14:25', '%Y-%m-%d%H:%M')
            if today_dt > d_time1:
                data_df['exp_date'] = today_dt.strftime('%Y-%m-%d')
            else:
                data_df['exp_date'] = (today_dt + td(-1)).strftime('%Y-%m-%d')
            data_df['update_time'] = today_dt.strftime('%Y-%m-%d %H:%M:%S')
            data_df['initiator'] = '补录'
            if self.flag_type:
                self.db_columns[self.type_db[3]].remove('ROI')
                self.db_columns[self.type_db[3]].remove('total_amount')
                data_df['shop_type'] = '京东商城'
            else:
                data_df['shop_type'] = '天猫商城'
            rename_dict = dict(zip(self.db_columns[self.type_db[2]],
                                   self.db_columns['om_market_t_tro_sku_gen'][:len(self.db_columns[self.type_db[2]])]))
            data_df.rename(columns=rename_dict, inplace=True)
            data_df.apply(self.data_insert, axis=1)

    def data_insert(self, data):
        sku_type = data['sku_type']
        data['sku_type'] = data['sku_type'].replace('kc', '0').replace('tp', '1').replace('ht', '2').replace('ztc',
                                                                                                             '3').replace(
            'reco', '4').replace('tbk', '5')
        old_shop_name = data['shop_name']
        data['shop_name'] = self.split_(data['shop_name'])
        i_sql = """        
        INSERT INTO {0} {1}VALUES{2};
        DELETE FROM {3} WHERE sku_id="{4}" AND sku_code="{5}" AND sku_type ="{6}" AND shop_name = "{7}";
        """.format(self.type_db[3], str(tuple([i[0] for i in data.keys()])).replace("'", ""),
                   str(tuple([i for i in data])), self.type_db[2],
                   *data[['goods_id', 'goods_no']], sku_type, old_shop_name)
        self.dc.bing_mysql(i_sql)

    def run(self):
        data = self.tra_paths()

        if data.shape[0]:
            data = pd.DataFrame(data, columns=self.columns)
            data.apply(self.tj_sku, axis=1)
            self.i_tgf()
            print(Fore.LIGHTGREEN_EX + '处理完毕', Style.RESET_ALL)
        else:
            print(Fore.LIGHTRED_EX + '数据未获取', Style.RESET_ALL)


if __name__ == '__main__':
    # flag True代表京东，False代表天猫
    flag = True
    dn = deal_none(flag)
    dn.run()
