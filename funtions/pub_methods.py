# -*- coding:utf-8 -*-
# @文件名称  :pub_methods
# @项目名称  :Promotion_Fee.py
# @软件名称  :PyCharm
# @创建时间  : 2021-10-19 14:27
# @用户名称  :DELL
import requests
from retrying import retry

from base_fun import funtion


class pub_method:
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

    @retry(stop_max_attempt_number=3)
    def get_contents(self, url, cookie, data=None):
        response = requests.get(url, data=data, headers=self.headers, cookies=cookie, stream=True)
        return response.content

    @retry(stop_max_attempt_number=3)
    def get_content(self, url, data, cookie):
        response = requests.post(url, data=data, headers=self.headers, cookies=cookie)
        return response.json()

    @staticmethod
    def create_file(base_route, br_file, file_name):
        fn = funtion.route_join(base_route, br_file)
        funtion.chect_dir(fn)
        return funtion.route_join(fn, '{0}'.format(file_name))