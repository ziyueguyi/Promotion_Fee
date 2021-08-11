# -*- coding:utf-8 -*-
# @文件名称  :funtion.py
# @项目名称  :Q4cal
# @软件名称  :PyCharm
# @创建时间  :2021-03-31 14:56
# @用户名称  :紫月孤忆
import datetime
import json
import logging
import os


def getOneday(ti=1):
    """
    获取昨天日期
    :param ti:
    :return:
    """
    today = datetime.date.today()
    o_day = datetime.timedelta(days=ti)
    yesterday = today - o_day
    return yesterday


def jud_path(d_path, flag=True, is_establish=True) -> tuple:
    """
    判断路径是否存在，不存在直接创建
    :param is_establish: 判断是否创建路径
    :param flag:判断是否提示
    :param d_path:文件路径，自动去除不可加入文件名的（.）字符
    :return:返回路径属性，存在返回True，不存在返回False和创建的路径
    """
    __dps = d_path.split('/')
    if __dps[-1].index('.') + 1 != len(__dps[-1]):
        __dps = os.path.join(*__dps[:-1]).replace('\\', '/')
    else:
        __dps = d_path
    if os.path.isdir(__dps):
        return True, ''
    else:
        __route = None
        if flag:
            print('创建路径：', __dps)
        if is_establish:
            os.makedirs(__dps)
            __route = __dps
        return False, __route


def chect_dir(route):
    if not os.path.exists(route):
        os.makedirs(route)
    return route


def route_join(*args):
    """
    路径拼接
    :param args:
    :return:
    """
    new_file = os.path.join(*args).replace('\\', '/')
    return new_file


def write_log(message, route, mod='deb'):
    """
    品牌新享日志
    :return:
    """
    dt = getOneday(1)
    month = dt.month
    year = dt.year
    filename = os.path.join(route, str(year)).replace('\\', '/')
    chect_dir(filename)
    if mod == 'deb':
        filename += '/deblog'
    elif mod == 'inf':
        filename += '/inflog'
    elif mod == 'war':
        filename += '/warlog'
    else:
        filename += '/crilog'

    filename += str(year) + '-' + str(month) + '.log'
    fmt = '%(asctime)s :(%(levelname)s) [line:%(lineno)d] %(message)s'
    level = "DEBUG"
    logger = None
    handler_file = None
    try:
        formatter = logging.Formatter(fmt)
        logger = logging.getLogger('myloger')
        logger.setLevel(logging.DEBUG)

        handler_file = logging.FileHandler(filename)
        handler_file.setFormatter(formatter)
        handler_file.setLevel(level)

        handler_console = logging.StreamHandler()
        handler_console.setFormatter(formatter)
        handler_console.setLevel(level)

        # 给logger添加handler
        logger.addHandler(handler_file)
        if mod == 'deb':
            logger.debug(message)
        elif mod == 'inf':
            logger.info(message)
        elif mod == 'war':
            logger.warning(message)
        else:
            logger.critical(message)
    except BaseException as e:
        print(e)
        print('日志写入错误，请及时检查，对已插入的')
    finally:
        logger.removeHandler(handler_file)  # 清除日志句柄里面的日志信息


def add_time(fun_c):
    def add_s_e_dt(*args, **kwargs):
        start_time = datetime.datetime.now()
        print('开始时间:', start_time.strftime('%Y-%m-%d %H:%M:%S'))
        fun_c(*args, **kwargs)
        end_time = datetime.datetime.now()
        print('结束时间:', end_time.strftime('%Y-%m-%d %H:%M:%S'))
        print('耗费时间:', end_time - start_time)

    return add_s_e_dt


# 保存cookie
def save_cookie(cookie, file_name):
    flag = False
    try:
        with open(file_name, 'w+', encoding="utf-8") as file:
            jud_path(file_name, flag=False)
            json.dump(cookie, file, ensure_ascii=False)
            flag = True
    except BaseException as e:
        ''.format(e)
        flag = False
    finally:
        return flag


# 读取cookie
def load_cookie(file_name):
    cookies_dict = dict()
    try:
        with open(file_name, 'r', encoding="utf-8") as file:
            listCookies = json.load(file)
        for cookie in listCookies:
            cookies_dict[cookie['name']] = cookie['value']
    except BaseException as e:
        ''.format(e)
    finally:
        return cookies_dict


# def cookies_load(shop_names):
#     """
#     处理cookies
#     :param shop_names:
#     :return:
#     """
#     cookies_dict = dict()
#     with open('./tool/datas/{0}/cookies/jd_{0}.json'.format(shop_names), 'r', encoding='utf-8') as f:
#         listCookies = json.loads(f.read())
#     for cookie in listCookies:
#         cookies_dict[cookie['name']] = cookie['value']
#     return cookies_dict


def get_route(str_data):
    """
    拆分日期为年月日
    :param str_data:
    :return:
    """
    ntCtime_dt = datetime.datetime.strptime(str_data, "%Y-%m-%d")  # str转datetime.datetime类型
    ntCtime = datetime.datetime.date(ntCtime_dt)
    return str(ntCtime.year), str(ntCtime.month), str(ntCtime.day)
