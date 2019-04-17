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
    从redis相应的key中获取代理ip(读取快代理的代理ip)
    :return:
    '''
    startup_nodes = [{'host': 'redis1', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    weibo_user_follower_proxy_length = r.llen('spider:weibo_user_follower:proxy:kuai')  # weibo_user_follower
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中weibo_user_follower的代理ip长度：', weibo_user_follower_proxy_length
    if weibo_user_follower_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    for i in xrange(weibo_user_follower_proxy_length):
        ip = r.lpop('spider:weibo_user_follower:proxy:kuai')
        proxies = {
            'http': "http://{ip}".format(ip=ip),
            'https': "http://{ip}".format(ip=ip)
        }

        PROXY_IP_Q.put(proxies)


def weibo_user_follower_type(lock, fileout):
    '''
    微博用户的粉丝分类信息
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

            weibo_user_follower_all_type_url_demo = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{weibo_user_id}'  # 获取微博用户的粉丝分类的url
            weibo_user_follower_all_type_url = weibo_user_follower_all_type_url_demo.format(weibo_user_id=weibo_user_id)
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '微博用户下粉丝分类url：', weibo_user_follower_all_type_url, proxies
            response = requests.get(url=weibo_user_follower_all_type_url, proxies=proxies, headers=headers, timeout=10)
            if response.status_code == 418:  # 针对该关键词没有微博内容的现象（服务器返回418状态码）
                print '该用户返回状态码为418：', weibo_user_id
                raise WeiboException('status code is 418')
            response_json = response.json()
            status = response_json.get('ok')
            if status == 1:
                data = response_json.get('data')
                if data:
                    cards = data.get('cards')
                    if cards:
                        cards_len = len(cards)  # 根据cards列表的长度判断该微博用户是否存在粉丝分类
                        # if cards_len == 1:
                        card_group = cards[0].get('card_group')    # 判断该用户是否存在粉丝
                        if card_group is None:    # 不存在粉丝
                            print '该用户没有粉丝：{weibo_user_id}。'.format(weibo_user_id=weibo_user_id)
                        else:    # 存在有粉丝  （没有粉丝）
                            # 是否存在粉丝分类
                            desc = card_group[0].get('desc')  # 描述
                            if desc is not None:
                                # print '微博用户下关注用户分类的描述：', desc
                                search_obj = re.search(r'\d+', desc, re.S)
                                if search_obj:   # 存在粉丝分类
                                    # num = search_obj.group()
                                    # print '微博用户下粉丝分类的粉丝数量：{num}。'.format(num=num)
                                    # all_page = int(num) / 20 + 1
                                    # print '微博用户下粉丝分类分页：{all_page}。'.format(all_page=all_page)
                                    if cards_len <= 4:  # 对于存在粉丝分类的微博用户就仅仅抓取粉丝分类的粉丝
                                        for card in cards:
                                            card_group = card.get('card_group')
                                            desc = card_group[0].get('desc')  # 描述
                                            if desc is not None:
                                                # print '微博用户下关注用户分类的描述：', desc
                                                search_obj = re.search(r'\d+', desc, re.S)
                                                if search_obj:
                                                    num = search_obj.group()
                                                    print '微博用户下粉丝分类的粉丝数量：{num}。'.format(num=num)
                                                    all_page = int(num) / 20 + 1
                                                    print '微博用户下粉丝分类分页：{all_page}。'.format(all_page=all_page)
                                                else:
                                                    print '通过正则解析微博用户下粉丝分类的粉丝用户数量失败：{weibo_user_id}。'.format(
                                                        weibo_user_id=weibo_user_id)
                                                    continue
                                                scheme = card_group[0].get('scheme')  # 微博用户下粉丝用户分类下的粉丝用户
                                                # print '微博用户下粉丝用户分类的url：', scheme
                                                if 'https://m.weibo.cn/p/index?' in scheme:
                                                    weibo_user_type_url_demo = scheme.replace(
                                                        'https://m.weibo.cn/p/index?',
                                                        'https://m.weibo.cn/api/container/getIndex?') + '&since_id={page}'  # 微博用户的粉丝用户各个分类url
                                                    print '微博用户的粉丝用户各个分类url的demo：', weibo_user_type_url_demo
                                                    weibo_user_follower(weibo_user_type_url_demo, all_page,
                                                                        weibo_user_id, proxies, lock, thread_name,
                                                                        fileout)  # 微博用户的粉丝用户信息接口
                                                else:
                                                    print '分析微博用户下粉丝用户分类的url的结构：{scheme}。'.format(scheme=scheme)
                                                    continue
                                    else:  # 对于存在粉丝分类的微博用户就仅仅抓取粉丝分类的粉丝
                                        for card in cards[0:3]:
                                            card_group = card.get('card_group')
                                            desc = card_group[0].get('desc')  # 描述
                                            if desc is not None:
                                                # print '微博用户下粉丝用户分类的描述：', desc
                                                search_obj = re.search(r'\d+', desc, re.S)
                                                if search_obj:
                                                    num = search_obj.group()
                                                    print '微博用户下粉丝分类的粉丝数量：{num}。'.format(num=num)
                                                    all_page = int(num) / 20 + 1
                                                    print '微博用户下粉丝分类分页：{all_page}。'.format(all_page=all_page)
                                                else:
                                                    print '通过正则解析微博用户下粉丝用户分类的粉丝用户数量失败：{weibo_user_id}。'.format(
                                                        weibo_user_id=weibo_user_id)
                                                    continue
                                                scheme = card_group[0].get('scheme')  # 微博用户下粉丝用户分类下的粉丝用户
                                                # print '微博用户下粉丝用户分类的url：', scheme
                                                if 'https://m.weibo.cn/p/index?' in scheme:
                                                    weibo_user_type_url_demo = scheme.replace(
                                                        'https://m.weibo.cn/p/index?',
                                                        'https://m.weibo.cn/api/container/getIndex?') + '&since_id={page}'  # 微博用户的粉丝用户各个分类url
                                                    print '微博用户的粉丝用户各个分类url的demo：', weibo_user_type_url_demo
                                                    weibo_user_follower(weibo_user_type_url_demo, all_page,
                                                                        weibo_user_id, proxies, lock, thread_name,
                                                                        fileout)  # 微博用户的粉丝用户信息接口
                                                else:
                                                    print '分析微博用户下粉丝用户分类的url的结构：{scheme}。'.format(scheme=scheme)
                                                    continue
                                else:
                                    card_last = cards[-1]
                                    card_group = card_last.get('card_group')
                                    no_type(card_group, weibo_user_id, lock, fileout)  # 无粉丝分类的粉丝信息提取接口

                            else:    # 不存在desc就是没有粉丝分类
                                card_last = cards[-1]
                                card_group = card_last.get('card_group')
                                no_type(card_group, weibo_user_id, lock, fileout)    # 无粉丝分类的粉丝信息提取接口

            else:
                print '微博用户下粉丝用户的分类url的data为空：{weibo_user_id}。'.format(weibo_user_id=weibo_user_id)

        except ReadTimeout as e:
            print 'ReadTimeout异常weibo_user_follower_type：', weibo_user_id
            WEIBO_USER_ID_QUEUE.put(weibo_user_id)

        except ValueError as e:
            print 'ValueError', weibo_user_id

        except AttributeError as e:
            print 'AttributeError：{weibo_user_id}'.format(weibo_user_id=weibo_user_id)

        except WeiboException as e:
            with lock:
                print 'WeiboException异常weibo_user_follower_type：', e.message
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectTimeout as e:
            with lock:
                print 'ConnectTimeout异常weibo_user_follower_type：', e.message
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量weibo_user_follower_type：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectionError as e:
            with lock:
                print 'ConnectionError异常weibo_user_follower_type：', e.message
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        # except BaseException as e:
        #     print 'BaseException异常weibo_user_follow_type'


def weibo_user_follower(weibo_user_type_url_demo, all_page, weibo_user_id, proxies, lock, thread_name, fileout):
    '''
    微博用户的粉丝用户信息
    :return:
    '''
    for page in xrange(1, all_page+1):
        try:
            weibo_user_follower_url = weibo_user_type_url_demo.format(page=page)
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '微博用户的粉丝用户url：', weibo_user_follower_url, proxies
            response = requests.get(url=weibo_user_follower_url, proxies=proxies, headers=headers, timeout=10)
            if response.status_code == 418:  # 针对该关键词没有微博内容的现象（服务器返回418状态码）
                print '微博用户的粉丝用户url返回状态码为418：', weibo_user_follower_url
                raise WeiboException('status code is 418')
            response_json = response.json()
            # print response_json
            status = response_json.get('ok')
            if status == 1:
                data = response_json.get('data')
                if data:
                    # cardlistInfo = data.get('cardlistInfo')
                    # follow_user_type = cardlistInfo.get('title_top').replace('\t', '')    # 粉丝用户类型
                    # print '粉丝用户类型：', follow_user_type

                    cards = data.get('cards')
                    if cards:
                        card_group = cards[0].get('card_group')
                        for card in card_group:
                            desc1 = card.get('desc1')    # 粉丝用户简介
                            if desc1 is None:
                                continue
                            else:
                                desc1 = desc1.replace('\t', '')
                            # print '粉丝用户简介：', desc1
                            scheme = card.get('scheme').replace('\t', '')    # 粉丝用户url
                            # print '粉丝用户url：', scheme
                            follow_count = card.get('user').get('friends_count')    # 粉丝用户关注数
                            # print '粉丝用户关注数：', follow_count
                            followers_count = card.get('user').get('followers_count')    # 粉丝用户粉丝数
                            # print '粉丝用户粉丝数：', followers_count
                            id = card.get('user').get('id')    # 粉丝用户id
                            # print '粉丝用户id：', id
                            screen_name = card.get('user').get('screen_name').replace('\t', '')    # 粉丝用户昵称
                            # print '粉丝用户昵称：', screen_name
                            content_json = {'weibo_user_id': weibo_user_id, 'scheme': scheme, 'id': id, 'screen_name': screen_name,
                                       'desc1': desc1, 'followers_count': followers_count, 'follow_count': follow_count}
                            content = json.dumps(content_json, ensure_ascii=False)
                            file_write(content, lock, fileout)
            else:
                print '微博用户下粉丝用户的data为空', weibo_user_id
                return None

        except ReadTimeout as e:
            print 'ReadTimeout异常weibo_user_follow'

        except WeiboException as e:
            with lock:
                print 'WeiboException异常：', e.message
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except SSLError as e:
            print 'SSLError异常'

        except ValueError as e:
            print 'ValueError异常weibo_user_follow：', e.message

        except ConnectTimeout as e:
            with lock:
                print 'ConnectTimeout异常weibo_user_follow：', e.message
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectionError as e:
            with lock:
                print 'ConnectionError异常weibo_user_follow：', e.message
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        # except BaseException as e:
        #     print 'BaseException异常weibo_user_follow'


def no_type(card_group, weibo_user_id, lock, fileout):
    for card_group1 in card_group:
        desc1 = card_group1.get('desc1')  # 粉丝简介
        # print '粉丝简介：', desc1
        scheme = card_group1.get('scheme')  # 粉丝用户url
        # print '粉丝用户url：', scheme
        followers_count = card_group1.get('user').get('followers_count')  # 粉丝粉丝数
        # print '粉丝粉丝数：', followers_count
        follow_count = card_group1.get('user').get('follow_count')  # 粉丝关注数
        # print '粉丝关注数：', follow_count
        id = card_group1.get('user').get('id')  # 粉丝id
        # print '粉丝id：', id
        screen_name = card_group1.get('user').get('screen_name')  # 粉丝昵称
        # print '粉丝昵称：', screen_name
        content_json = {'weibo_user_id': weibo_user_id, 'desc1': desc1, 'scheme': scheme,
                        'followers_count': followers_count, 'follow_count': follow_count,
                        'id': id, 'screen_name': screen_name}
        content = json.dumps(content_json, ensure_ascii=False)
        file_write(content, lock, fileout)


def file_write(content, lock, fileout):
    '''
    写入文件
    '''
    with lock:
        fileout.write(content)
        fileout.write('\n')
        fileout.flush()


def main():
    lock = threading.Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'start'
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')  # 来源文件日期
    file_time = time.strftime('%Y%m')  # 生成文件日期

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
        search_obj = re.search(r'\d+', line, re.S)    # 提取微博用户id
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
    dest_file_name = os.path.join(dest_path, 'weibo_user_follower_' + file_time)
    tmp_file_name = os.path.join(dest_path, 'weibo_user_follower_' + file_time + '.tmp')
    fileout = open(tmp_file_name, 'a')

    threads = []
    for i in range(50):
        t = threading.Thread(target=weibo_user_follower_type, args=(lock, fileout))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(2)
        fileout.close()
    os.rename(tmp_file_name, dest_file_name)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '抓取结束'


if __name__ == '__main__':
    main()

