# coding:utf-8
__author__ = 'xxj'

import requests
import time
import threading
import json
import math
import datetime
import Queue
import re
import os
from rediscluster import StrictRedisCluster
import lxml.etree
from Queue import Empty
from requests.exceptions import ReadTimeout, ConnectionError, SSLError, ConnectTimeout
from exceptions import ValueError
import sys

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
    }
WEIBO_USER_ID_QUEUE = Queue.Queue()    # 微博用户id队列
DATA_QUEUE = Queue.Queue()    # 数据存储队列
PROXY_IP_Q = Queue.Queue()    # 代理IP队列
THREAD_PROXY_MAP = {}    # 线程与代理关系


class WeiboException(Exception):
    def __init__(self, message):
        super(WeiboException, self).__init__(self)
        self.message = message


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip
    :return:
    '''
    startup_nodes = [{'host': 'redis1', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    weibo_user_proxy_length = r.llen('spider:weibo_user:proxy')  # weibo_user
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中weibo_user的代理ip长度：', weibo_user_proxy_length
    if weibo_user_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    for i in xrange(weibo_user_proxy_length):
        ip = r.lpop('spider:weibo_user:proxy')
        proxies = {
            'http': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip),
            'https': "http://8c84700fa7d2:kgvavaeile@{ip}".format(ip=ip)
        }
        PROXY_IP_Q.put(proxies)


def weibo_user_info(lock):
    '''
    微博用户信息
    :return:
    '''
    while not WEIBO_USER_ID_QUEUE.empty():
        try:
            weibo_user_id = WEIBO_USER_ID_QUEUE.get(False)
            print '微博用户id：', weibo_user_id

            thread_name = threading.currentThread().name  # 当前线程名
            if not THREAD_PROXY_MAP.get(thread_name):
                THREAD_PROXY_MAP[thread_name] = PROXY_IP_Q.get(False)
            proxies = THREAD_PROXY_MAP.get(thread_name)

            weibo_user_url_demo = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={weibo_user_id}'    # 获取微博用户信息
            weibo_user_url = weibo_user_url_demo.format(weibo_user_id=weibo_user_id)
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '微博用户信息url：', weibo_user_url, proxies
            response = requests.get(url=weibo_user_url, proxies=proxies, headers=headers, timeout=10)
            if response.status_code == 418:  # 针对该关键词没有微博内容的现象（服务器返回418状态码）
                print '微博用户信息:该用户返回状态码为418：', weibo_user_id
                raise WeiboException('status code is 418')
            response_json = response.json()
            status = response_json.get('ok')
            if status == 1:
                data = response_json.get('data')
                if data:
                    userInfo = data.get('userInfo')
                    if userInfo:
                        follow_count = userInfo.get('follow_count')    # 关注数
                        # print '关注数：', follow_count
                        followers_count = userInfo.get('followers_count')    # 粉丝数
                        # print '粉丝数：', followers_count
                        id = userInfo.get('id')    # 用户id
                        # print '用户id：', id
                        screen_name = userInfo.get('screen_name')    # 用户昵称
                        # print '用户昵称：', screen_name
                        statuses_count = userInfo.get('statuses_count')    # 用户发的微博数
                        # print '用户的微博数：', statuses_count
                        verified_reason = userInfo.get('verified_reason')    # 新浪认证
                        if verified_reason is None:
                            verified_reason = ''
                        # print '新浪认证：', verified_reason
                        weibo_user_info = {'id': id, 'screen_name': screen_name, 'follow_count': follow_count, 'followers_count': followers_count, 'statuses_count': statuses_count, 'verified_reason': verified_reason}    # 微博用户信息

            else:
                print '微博用户信息url的data为空', weibo_user_id, '返回的状态码：', status
                continue

            weibo_user_basic_info_dict = weibo_user_basic_info(weibo_user_id, proxies)    # 微博用户基本信息接口
            if weibo_user_basic_info_dict:
                weibo_user_info.update(weibo_user_basic_info_dict)
            else:
                print '微博用户基本信息检测：', weibo_user_id
            DATA_QUEUE.put(weibo_user_info)

        except ReadTimeout as e:
            print 'ReadTimeout异常：', weibo_user_id
            WEIBO_USER_ID_QUEUE.put(weibo_user_id)
	
        except ValueError as e:
            print 'ValueError异常：', weibo_user_id, e.message

        except WeiboException as e:
            with lock:
                print 'WeiboException异常：', e.message    # 两种封ip现象：1、status code is 418；2、error is 100005
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectTimeout as e:
            with lock:
                print 'ConnectTimeout异常：', weibo_user_id
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量weibo_user_follow_type：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectionError as e:
            with lock:
                print 'ConnectionError异常：', weibo_user_id
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies


def weibo_user_basic_info(weibo_user_id, proxies):
    '''
    用户基本信息
    :param weibo_user_id:
    :return:
    '''
    weibo_user_basic_info_url_demo = 'https://m.weibo.cn/api/container/getIndex?containerid=230283{weibo_user_id}_-_INFO&title=基本资料'
    weibo_user_basic_info_url = weibo_user_basic_info_url_demo.format(weibo_user_id=weibo_user_id)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '用户基本信息url：', weibo_user_basic_info_url, proxies
    response = requests.get(url=weibo_user_basic_info_url, headers=headers, proxies=proxies, timeout=10)
    if response.status_code == 418:  # 针对该关键词没有微博内容的现象（服务器返回418状态码）
        print '用户基本信息:该用户返回状态码为418：', weibo_user_id
        raise WeiboException('status code is 418')
    response_json = response.json()
    status = response_json.get('ok')
    weibo_user_basic_info_dict = {}    # 微博用户基本信息
    if status == 1:
        data = response_json.get('data')
        if data:
            cards = data.get('cards')
            if cards:
                card1 = cards[0]
                card_group1 = card1.get('card_group')
                description_name = card_group1[-1].get('item_name')
                description = card_group1[-1].get('item_content')  # 简介
                # print description_name, description
                if description_name == u'简介':
                    description_name = 'introduction'
                weibo_user_basic_info_dict[description_name] = description

                card2 = cards[1]
                card_group2 = card2.get('card_group')
                for card in card_group2[1:]:
                    item_name = card.get('item_name')    # 属性
                    item_content = card.get('item_content')    # 内容
                    # print '属性：', item_name, '内容：', item_content
                    sx_dict = {u'所在地': 'location', u'性别': 'sex'}
                    item_name = sx_dict.get(item_name)
                    weibo_user_basic_info_dict[item_name] = item_content
        print weibo_user_basic_info_dict
    else:
        print '用户基本信息url的data为空', weibo_user_id, '返回的状态码：', status
    return weibo_user_basic_info_dict


def file_write(f, lock):
    '''
    写入文件
    :param f:
    :return:
    '''
    while (not WEIBO_USER_ID_QUEUE.empty()) or (not DATA_QUEUE.empty()):
        try:
            weibo_user_info = DATA_QUEUE.get(False)
            content = json.dumps(weibo_user_info, ensure_ascii=False)
            with lock:
                f.write(content)
                f.write('\n')
                f.flush()
        except Empty as e:
            pass


def file_write_other(f):
    '''
    写入文件
    :param f:
    :return:
    '''
    while not DATA_QUEUE.empty():
        try:
            weibo_user_info = DATA_QUEUE.get(False)
            content = json.dumps(weibo_user_info, ensure_ascii=False)
            f.write(content)
            f.write('\n')
            f.flush()
        except Empty as e:
            pass


def main():
    lock = threading.Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'start'
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')    # 来源文件日期
    file_time = time.strftime('%Y%m')    # 生成文件日期

    keyword_file_dir = r'/ftp_samba/112/file_4spider/weibo_url/'  # weibo_url的来源目录
    keyword_file_name = r'weibo_url_{date}.txt'.format(date=date)  # weibo_url的来源文件名
    keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
    # keyword_file_path = r'C:\Users\xj.xu\Desktop\weibo_url.txt'
    # keyword_file_path = '/home/tools/spider/python/test/weibo_url.txt'
    print '获取来源文件：', keyword_file_path
    if not os.path.exists(keyword_file_path):  # 当前文件不存在，获取前一天的最后一个文件
        keyword_file_name = os.listdir(keyword_file_dir)[-1]
        keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
        print '源文件不存在，获取当前最新文件：', keyword_file_path
    keyword_file = open(keyword_file_path, 'r')
    for line in keyword_file:
        line = line.strip()
        search_obj = re.search(r'\d+', line, re.S)
        if search_obj:
            weibo_user_id = search_obj.group()
            WEIBO_USER_ID_QUEUE.put(weibo_user_id)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '数据来源关键词的数量：', WEIBO_USER_ID_QUEUE.qsize()

    get_redis_proxy()  # 将redis中的代理ip放入到PROXY_IP_Q队列中
    proxy_count = PROXY_IP_Q.qsize()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip队列中的ip数量：', proxy_count

    dest_path = '/ftp_samba/112/spider/python/weibo/'  # linux上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'weibo_user_info_' + file_time)
    tmp_file_name = os.path.join(dest_path, 'weibo_user_info_' + file_time + '.tmp')
    fileout = open(tmp_file_name, 'a')

    threads = []
    for i in range(50):
        t = threading.Thread(target=weibo_user_info, args=(lock,))
        t.start()
        threads.append(t)

    data_threads = []
    for i in xrange(50):
        t = threading.Thread(target=file_write, args=(fileout, lock))
        t.start()
        data_threads.append(t)

    for t in threads:
        t.join()

    for t in data_threads:
        t.join()

    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '数据存储队列的长度：', DATA_QUEUE.qsize()
    if not DATA_QUEUE.empty():
        file_write_other(fileout)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '筛选之后数据队列的数量', DATA_QUEUE.qsize()
    fileout.flush()
    try:
        fileout.close()
    except IOError as e:
        time.sleep(2)
        fileout.close()
    os.rename(tmp_file_name, dest_file_name)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '抓取结束'


if __name__ == '__main__':
    main()

