# -*- coding:utf-8 -*-
# @文件名称  :ceshi.py
# @项目名称  :Promotion_Fee
# @软件名称  :PyCharm
# @创建时间  :2021-04-06 10:54
# @用户名称  :紫月孤忆
import json
import yamail
import datetime
import requests
from datetime import datetime
from os.path import exists


class Send:
    def __init__(self, e_mail=None, c_mail=None, up=None, wx_key=None, per=None):
        """
        发送信息的基础配置
        :param e_mail: 接收人邮箱
        :param c_mail: 抄送人邮箱
        :param up: 邮箱的账户密码配置
        :param wx_key: 企业微信机器人秘钥
        :param per: 企业微信需要@的人
        """
        if e_mail is None:
            e_mail = ['2275914231@qq.com']
        if c_mail is None:
            c_mail = ['2275914231@qq.com']
        if wx_key is None:
            wx_key = '38bd063a-f79b-47b1-a27d-06c6c88476ee'
        if per is None:
            per = ['@all']
        if up is None:
            up = ('17630583910@163.com', 'DYKEELXEYILMTFNP')
        self.USER_MAIL_LIST = e_mail
        self.USER_MAIL_CC = c_mail
        self.MAIL_INFO = {
            'host': 'smtp.163.com',
            'user': up[0],
            'password': up[1],
            'port': 465
        }  # 邮箱信息
        self.WX_TYPE = 'send'
        self.WX_KEY = wx_key
        self.WX_URL = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/{0}?key={1}{2}'
        self.PER = per

    def send_mail(self, title, content, file=None):
        try:
            smtp = yamail.SMTP(**self.MAIL_INFO)
            smtp.send(to=self.USER_MAIL_LIST, cc=self.USER_MAIL_CC,
                      subject=title, contents=content,
                      attachments=file)
        except Exception as e:
            print(e)

    def send_msg(self, wx_content):
        """
        企业微信发送消息
        :param wx_content:
        :return:
        """
        # 数据部机器人
        now_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        send_messages = now_time + ':' + wx_content
        string_text = {
            "msgtype": "text",
            "text": {"content": send_messages,
                     "mentioned_mobile_list": self.PER,
                     }
        }
        # 编码转换
        data = json.dumps(string_text, ensure_ascii=False).encode('utf-8')
        headers = {'Content-Type': 'application/json'}

        res = requests.post(self.WX_URL.format(self.WX_TYPE, self.WX_KEY, ''), data=data, headers=headers)
        response = res.json()['errmsg']
        return '运行成功' if response == 'ok' else '推送成功'

    def send_file(self, file_path):
        """
        企业微信发送文件
        :param file_path:
        :return:
        """
        if exists(file_path):
            try:
                id_url = self.WX_URL.format('upload_media', self.WX_KEY, '&type=file')
                wx_url = self.WX_URL.format(self.WX_TYPE, self.WX_KEY, '')
                data = {'media': open(file_path, 'rb')}
                mess = requests.post(url=id_url, files=data)
                dict_data = mess.json()
                media_id = dict_data['media_id']
                data = {
                    "msgtype": "file",
                    "file": {"media_id": media_id}
                }
                r = requests.post(url=wx_url, json=data)
            except BaseException as e:
                return e is not None
            return True
        else:
            return False
