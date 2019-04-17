# coding:utf-8
__author__ = 'xxj'

import requests
import time
import threading
import json
import math
import Queue
import hashlib
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

NICK_NAME_QUEUE = Queue.Queue()    # 昵称队列
THREAD_PROXY_MAP = {}    # 线程与代理关系
PROXY_IP_Q = Queue.Queue()    # 代理ip队列
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
    pubg_match_proxy_length = r.llen('spider:pubg_match:proxy:kuai')  # pubg_match
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中pubg_match的代理ip长度：', pubg_match_proxy_length
    if pubg_match_proxy_length >= 50:
        proxy_length = 50
    else:
        print '当前快代理redis中的代理数量少于50：', r.llen('spider:pubg_match:proxy:kuai')
        time.sleep(60)
        return get_redis_proxy()

    for i in xrange(proxy_length):
        ip = r.lpop('spider:pubg_match:proxy:kuai')
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
    :return:
    '''
    while True:
        current_time = int(time.strftime('%H%M%S'))
        if 001000 >= current_time >= 000000:  # 退出进程
            print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'pubg_userid()退出'
            return False
        try:
            nick_name = r.lpop('spider:python:pubg_match:keyword')    # 用户昵称
            if nick_name:
                thread_name = threading.currentThread().name  # 当前线程名
                if not THREAD_PROXY_MAP.get(thread_name):
                    THREAD_PROXY_MAP[thread_name] = PROXY_IP_Q.get(False)
                proxies = THREAD_PROXY_MAP.get(thread_name)

                url = 'https://pubg.op.gg/user/{nick_name}'.format(nick_name=nick_name)
                print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '获取user_id的url: ', url, proxies
                response = requests.get(url=url, headers=headers, proxies=proxies, timeout=30)
                response_obj = lxml.etree.HTML(response.text)
                user_id = response_obj.xpath('//div[@id="userNickname"]/@data-user_id')  # 用户id
                if user_id:
                    user_id = user_id[0]
                    pubg_match_url = 'https://pubg.op.gg/api/users/{user_id}/matches/recent'.format(user_id=user_id)    # 第一页
                    pubg_match(user_id, pubg_match_url, proxies, r)  # 用户的比赛信息接口

                else:  # 不存在该用户或者该用户没有user_id
                    user_id = ''
                    print '不存在该用户或该用户没有userid：', nick_name
            else:
                print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '当前redis中无nickname', '等待2小时'

        except ConnectionError as e:
            with lock:
                print 'ConnectionError：', nick_name, e.message
                # if 'Read timed out' in repr(e.message):    # 请求超时     (thread---ip)
                r.rpush('spider:python:pubg_match:keyword', nick_name)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies

        except ReadTimeout as e:    # 请求超时
            with lock:
                print 'ReadTimeout：', nick_name, e.message
                r.rpush('spider:python:pubg_match:keyword', nick_name)

        except BaseException as e:
            with lock:
                print 'BaseException：', nick_name, e.message
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                print '新的代理IP：', proxies
                THREAD_PROXY_MAP[thread_name] = proxies


def pubg_match(user_id, url, proxies, r):
    '''
    玩家玩的比赛的赛事id（用户id，每一页的url， 代理）
    :return:
    '''
    try:
        current_time = int(time.strftime('%H%M%S'))
        if 001000 >= current_time >= 000000:  # 退出进程
            print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'pubg_match()退出'
            return False
        # url = 'https://pubg.op.gg/api/users/{user_id}/matches/recent'.format(user_id=user_id)
        print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '玩家的每页比赛的url： ', url, proxies
        response = requests.get(url=url, headers=headers, proxies=proxies, timeout=10)
        response_json = response.json()
        matches = response_json.get('matches')
        if matches:
            items = matches.get('items')
            if items:    # 玩家一页的比赛
                for item1 in items:  # 每个item1表示该用户的一场比赛
                    match_id = item1.get('match_id')  # 赛事id
                    mode = item1.get('mode')  # 游戏模式
                    offset = item1.get('offset')  # after所需字段
                    queue_size = item1.get('queue_size')  # 游戏每队人数
                    server = item1.get('server')  # 区服
                    started_at = item1.get('started_at').replace('T', ' ').replace('+0000', '')  # 赛事时间 时间格式：2018-10-02T05:53:15+0000
                    total_rank = item1.get('total_rank')  # 总队伍数
                    type = item1.get('type')  # 游戏类型
                    # match表示该玩家的一场比赛的赛事信息
                    match = {'match_id': match_id,
                             'mode': mode,
                             # 'offset': offset,
                             'queue_size': queue_size,
                             'server': server,    # 查看数据，是否有区服
                             'started_at': started_at,
                             'total_rank': total_rank,
                             'type': type
                             }
                    match = json.dumps(match, ensure_ascii=False)
                    # MATCH_DATA_QUEUE.put(match)
                    r.lpush('spider:python:pubg_match:keyword:dest', match)
		    # 内存不足
                    match_id_md5 = hashlib.md5(match_id).hexdigest()  # 对match_id进行md5加密
                    if not r.sismember('spider:python:pubg_match_match_id:keyword:{mid}'.format(mid=match_id_md5[:2]), match_id_md5):
                        r.sadd('spider:python:pubg_match_match_id:keyword:{mid}'.format(mid=match_id_md5[:2]), match_id_md5)
                        user_game_url = 'https://pubg.op.gg/api/matches/{match_id}'.format(match_id=match_id.replace('=', '%3D'))  # 一场游戏中所有团队的信息
                        user_game(user_game_url, match_id, proxies, r)    # 一场游戏中所有团队的信息
		    #user_game_url = 'https://pubg.op.gg/api/matches/{match_id}'.format(match_id=match_id.replace('=', '%3D'))  # 一场游戏中所有团队的信息
                    #user_game(user_game_url, match_id, proxies, r)    # 一场游戏中所有团队的信息
                game_id_url = 'https://pubg.op.gg/api/users/{user_id}/matches/recent?after={after}'.format(user_id=user_id, after=offset.replace('=', '%3D'))  # 该用户第二页及之后页面比赛信息url
                pubg_match(user_id, game_id_url, proxies, r)

    except BaseException as e:
        print 'pubg_match方法中出现BaseException异常', e


def user_game(user_game_url, match_id, proxies, r):
    '''
    一场游戏中所有团队的信息(url， 比赛id, 代理)
    :param user_game_url:
    :return: 一场比赛的所有团队信息
    '''
    try:
        current_time = int(time.strftime('%H%M%S'))
        if 001000 >= current_time >= 000000:  # 退出进程
            print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'user_game()退出'
            return False
        print time.strftime('[%Y-%m-%d %H:%M:%S]：'), '一场比赛所有团队的url ', user_game_url, proxies
        response = requests.get(url=user_game_url, headers=headers, proxies=proxies, timeout=10)
        response_json = response.json()
        mode = response_json.get('mode')  # 游戏模式
        queue_size = response_json.get('queue_size')  # 队友个数
        started_at = response_json.get('started_at')  # 游戏时间
        user_games = []    # 一场游戏所有团队的信息存放在list中
        teams = response_json.get('teams')  # 团队列表
        if teams:    # 一场比赛所有的团队
            for team in teams:    # 遍历一场比赛中的每一个团队
                participants = team.get('participants')  # 获取一个团队中的玩家（list）
                if participants:
                    user_games.append(participants)  # 元素结构为：外层列表为一场游戏（单个元素为该场游戏的某个团队），内层列表为一个团队（单个元素为团队成员相关信息）
        else:
            user_games = []

        pubg_user_game = {
            'match_id': match_id,
            'mode': mode,
            'queue_size': queue_size,
            'started_at': started_at,
            'user_games': user_games,
        }
        pubg_user_game = json.dumps(pubg_user_game, ensure_ascii=False)
        # GAME_DATA_QUEUE.put(pubg_user_game)
        r.lpush('spider:python:pubg_game:keyword:dest', pubg_user_game)

    except BaseException as e:
        print 'pubg_game方法出现BaseException异常', e


def main():
    lock = threading.Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]：'), 'start'

    startup_nodes = [{'host': 'redis3', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    print 'redis中pubg_match昵称列表长度：', r.llen('spider:python:pubg_match:keyword')

    get_redis_proxy()  # 将redis中的代理ip放入到PROXY_IP_Q队列中
    proxy_count = PROXY_IP_Q.qsize()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip队列中的ip数量：', proxy_count
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '开启的线程数：', proxy_count

    threads = []
    for i in xrange(50):
        t = threading.Thread(target=pubg_userid, args=(lock, r))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print '源数据剩余量：', r.llen('spider:python:pubg_match:keyword')

    print '抓取结束'


if __name__ == '__main__':
    main()
