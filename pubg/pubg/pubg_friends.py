# coding:utf-8
__author__ = 'xxj'

import requests
import time
import threading
import json
import Queue
from rediscluster import StrictRedisCluster
import lxml.etree
from Queue import Empty
from requests.exceptions import ReadTimeout, ConnectionError, ConnectTimeout, ProxyError, ChunkedEncodingError
from exceptions import ValueError
import re
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
    }

KEYWORD_QUEUE = Queue.Queue()    # 数据源队列
DATA_QUEUE = Queue.Queue()    # 数据存储队列
PROXY_IP_Q = Queue.Queue()    # 代理IP队列
THREAD_PROXY_MAP = {}    # 线程与代理关系
season = time.strftime('%Y-%m')    # 用户的玩家列表url中season参数


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip(读取快代理的代理ip)
    :return:
    '''
    current_time = int(time.strftime('%H%M%S'))
    if 001000 >= current_time >= 000000:  # 退出进程
        print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'get_redis_proxy()退出'
        return False
    startup_nodes = [{'host': 'redis3', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    pubg_friends_proxy_length = r.llen('spider:pubg_friends:proxy:kuai')  # pubg_friends
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中pubg_friends的代理ip长度：', pubg_friends_proxy_length
    if pubg_friends_proxy_length >= 50:
        proxy_length = 50
    else:
        print '当前快代理redis中的代理数量少于50：', r.llen('spider:pubg_friends:proxy:kuai')
        time.sleep(60)
        return get_redis_proxy()

    for i in xrange(proxy_length):
        ip = r.lpop('spider:pubg_friends:proxy:kuai')
        if ip:
            proxies = {
                'http': "http://{ip}".format(ip=ip),
                'https': "http://{ip}".format(ip=ip)
            }
            PROXY_IP_Q.put(proxies)
    ip_num = PROXY_IP_Q.qsize()  # 代理ip队列中的ip数量
    if ip_num < 50:
        print '代理IP队列中的ip数量少于50个：', ip_num
        time.sleep(20)
        return get_redis_proxy()


def pubg_userid(lock, r):
    '''
    根据pubg用户的昵称获取用户的userid
    :param nick_name:
    :return:
    '''
    while True:
        current_time = int(time.strftime('%H%M%S'))
        if 001000 >= current_time >= 000000:    # 退出进程
            print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'pubg_userid()退出'
            return False
        try:
            nick_name = r.lpop('spider:python:pubg_friends:keyword')    # 用户昵称
            if nick_name:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36'
                }

                thread_name = threading.currentThread().name  # 当前线程名
                if not THREAD_PROXY_MAP.get(thread_name):
                    THREAD_PROXY_MAP[thread_name] = PROXY_IP_Q.get(False)
                proxies = THREAD_PROXY_MAP.get(thread_name)

                url = 'https://pubg.op.gg/user/{nick_name}'.format(nick_name=nick_name)
                print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '获取user_id的url： ', url, proxies
                response = requests.get(url=url, headers=headers, proxies=proxies, timeout=30)
                response_obj = lxml.etree.HTML(response.text)
                user_id = response_obj.xpath('//div[@id="userNickname"]/@data-user_id')    # 用户id
                if user_id:
                    user_id = user_id[0]    # 用户id
                    friends_info = pubg_friend(user_id, proxies)    # 用户的玩家信息
                    # print 'friends_info：', friends_info
                    content = {'nick_name': nick_name,    # 用户昵称
                               'user_id': user_id,    # 用户id
                               'season': season,    # 赛季
                               'friends_info': friends_info,    # 用户好友信息
                               'server': 'pc-as'    # server
                               }
                    # print content
                    content_str = json.dumps(content, ensure_ascii=False)
                    # DATA_QUEUE.put(content_str)
                    r.lpush('spider:python:pubg_friends:keyword:dest', content_str)
                else:    # 不存在该用户或者该用户没有user_id
                    user_id = ''
                    print '不存在该用户或该用户没有userid：', nick_name
            else:
                print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '当前redis中无nickname', '等待2小时'
        except ConnectionError as e:
            with lock:
                print 'ConnectionError：', nick_name, e.message
                # if 'Read timed out' in repr(e.message):    # 请求超时     (thread---ip)
                r.rpush('spider:python:pubg_friends:keyword', nick_name)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies

        except ReadTimeout as e:    # 请求超时(还需要确定这种情况是否需要切换代理ip)
            with lock:
                print 'ReadTimeout：', nick_name, e.message
                r.rpush('spider:python:pubg_friends:keyword', nick_name)

        except ChunkedEncodingError as e:
            print 'ChunkedEncodingError:', e.message, nick_name

        except BaseException as e:
            with lock:
                print 'BaseException：', e.message, nick_name
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies


def pubg_friend(user_id, proxies):
    '''
    根据用户的userid获取该用户的好友列表
    :param user_id:
    :return:
    '''
    # url = 'https://pubg.op.gg/api/users/{user_id}/matches/summary-played-with?season=2018-06&server=as'.format(user_id=user_id)
    url = 'https://pubg.op.gg/api/users/{user_id}/matches/summary-played-with?season=pc-2018-01'.format(user_id=user_id)
    print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '获取好友列表url： ', url, proxies
    response = requests.get(url=url, headers=headers, proxies=proxies, timeout=10)
    # print response.url
    response_json = response.json()
    friend_info_ls = []
    users = response_json.get('users')    # 用户游戏好友列表
    if users:  # 为了存储数据内容
        for userx in users:  # 获取单个游戏好友
            user = userx.get('user')
            stats = userx.get('stats')
            if user and stats:
                steam_id = user.get('identity_id')  # 存在None情况
                if steam_id:
                    steam_id = steam_id.replace('identity.steam.', '')  # steam_id
                else:
                    steam_id = ''
                nick_name = user.get('nickname')  # 昵称
                if not nick_name:
                    nick_name = ''
                matches_count = stats.get('matches_count')  # 一起游戏次数
                if not matches_count:
                    matches_count = ''
                friends_infor = {  # 用户friends的相关信息
                    'steam_id': steam_id,    # steam_id
                    'nick_name': nick_name,    # 游戏好友昵称
                    'matches_count': matches_count,    # 一起玩游戏的次数
                }
                friend_info_ls.append(friends_infor)
        return friend_info_ls
    else:
        return friend_info_ls


def main():
    lock = threading.Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'start'

    startup_nodes = [{'host': 'redis3', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    print 'redis中pubg_friends昵称列表长度：', r.llen('spider:python:pubg_friends:keyword')

    get_redis_proxy()  # 将redis中的代理ip放入到PROXY_IP_Q队列中
    proxy_count = PROXY_IP_Q.qsize()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip队列中的ip数量：', proxy_count

    threads = []
    for i in range(50):
        t = threading.Thread(target=pubg_userid, args=(lock, r))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print '源数据剩余量：', r.llen('spider:python:pubg_friends:keyword')

    print '抓取结束'


if __name__ == '__main__':
    main()
