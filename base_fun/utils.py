# -*- coding: utf-8 -*-
# @Time    : 2021/1/11 15:39
# @Author  : Paul
# pip install nacos-sdk-python
# 数据库连接账号密码产生器，禁止私自运行
import json
import nacos


def get_mysql_msg(db_type):
    SERVER_ADDRESSES = "47.103.106.151:8488"
    NAMESPACE = "241701d8-723d-41ce-a1ec-08810a834788"

    # no auth mode
    # client = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE)
    # auth mode
    # client = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE, username="nacos", password="nacos")
    client = nacos.NacosClient(SERVER_ADDRESSES, namespace=NAMESPACE, username="python_nacos", password="python")

    # get config
    if db_type == 'dev':
        data_id = "python_db_test"
        group = "DEV_GROUP"
    elif db_type == 'prod':
        data_id = "python_db_prod"
        group = "PROD_GROUP"
    else:
        return {"msg": "wrong_type"}

    group_msg = client.get_config(data_id, group)
    res = json.loads(group_msg)

    return res


print(get_mysql_msg('dev'))
