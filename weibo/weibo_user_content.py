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
    weibo_user_content_proxy_length = r.llen('spider:weibo_user_content:proxy:kuai')  # weibo_user_content
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中weibo_user_content的代理ip长度：', weibo_user_content_proxy_length
    if weibo_user_content_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    for i in xrange(weibo_user_content_proxy_length):
        ip = r.lpop('spider:weibo_user_content:proxy:kuai')
        proxies = {
            'http': "http://{ip}".format(ip=ip),
            'https': "http://{ip}".format(ip=ip)
        }

        PROXY_IP_Q.put(proxies)


def weibo_user_content_num(lock, fileout):
    '''
    微博用户发表微博数量
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

            weibo_user_content_num_url_demo = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={weibo_user_id}&containerid=107603{weibo_user_id}'    # 获取微博用户的微博总数
            weibo_user_content_num_url = weibo_user_content_num_url_demo.format(weibo_user_id=weibo_user_id)
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '微博用户的微博总数url：', weibo_user_content_num_url, proxies
            response = requests.get(url=weibo_user_content_num_url, proxies=proxies, headers=headers, timeout=10)
            if response.status_code == 418:  # 针对该关键词没有微博内容的现象（服务器返回418状态码）
                print '该用户返回状态码为418：', weibo_user_id
                raise WeiboException('status code is 418')
            response_json = response.json()
            status = response_json.get('ok')
            if status == 1:
                data = response_json.get('data')
                if data:
                    cardlistInfo = data.get('cardlistInfo')
                    total = cardlistInfo.get('total')
                    # print 'total：', total
                    print '该微博用户{weibo_user_id}发表的微博数：{total}。'.format(total=total, weibo_user_id=weibo_user_id)
                    all_page = total / 10
                    print '该用户的总页数：', all_page
                    if all_page > 10:    # 只抓取前十页
                        all_page = 10
                    weibo_user_content_url_demo = 'https://m.weibo.cn/api/container/getIndex?type=uid&value={weibo_user_id}&containerid=107603{weibo_user_id}&page={page}'
                    weibo_user_content(all_page, weibo_user_content_url_demo, weibo_user_id, proxies, lock, thread_name, fileout)    # 微博内容获取接口

            else:
                print '微博用户发表微博数量url的data为空：{weibo_user_id}。'.format(weibo_user_id=weibo_user_id)

        except ReadTimeout as e:
            print 'ReadTimeout异常weibo_user_content_num：', weibo_user_id
            WEIBO_USER_ID_QUEUE.put(weibo_user_id)

        except ValueError as e:
            print 'ValueError', weibo_user_id

        except WeiboException as e:
            with lock:
                print 'WeiboException异常weibo_user_content_num：', e.message
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectTimeout as e:
            with lock:
                print 'ConnectTimeout异常weibo_user_content_num：', e.message
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量weibo_user_content_num：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectionError as e:
            with lock:
                print 'ConnectionError异常weibo_user_content_num：', e.message
                WEIBO_USER_ID_QUEUE.put(weibo_user_id)
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies


def weibo_user_content(all_page, weibo_user_content_url_demo, weibo_user_id, proxies, lock, thread_name, fileout):
    for page in xrange(1, all_page+1):
        try:
            weibo_user_content_url = weibo_user_content_url_demo.format(weibo_user_id=weibo_user_id, page=page)    # 遍历微博用户每一页的微博内容
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '微博用户的微博url：', weibo_user_content_url, proxies
            response = requests.get(url=weibo_user_content_url, proxies=proxies, headers=headers, timeout=10)
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
                        for card in cards:
                            mblog = card.get('mblog')
                            if mblog is not None:    # 在cards列表中存在非微博内容的元素
                                comments_count = mblog.get('comments_count')    # 评论数
                                # print '评论数：', comments_count
                                created_at = mblog.get('created_at')    # 发布日期
                                # print '发布日期：', created_at
                                reposts_count = mblog.get('reposts_count')    # 转发数
                                # print '转发数：', reposts_count
                                attitudes_count = mblog.get('attitudes_count')  # 点赞数
                                # print '点赞数：', attitudes_count
                                text = mblog.get('text')  # 发布微博内容
                                text_obj = lxml.etree.HTML(text)
                                if text_obj is not None:
                                    text = text_obj.xpath('string(.)')
                                else:
                                    text = ''
                                # print '发布微博内容：', text
                                weibo_info = {'weibo_user_id': weibo_user_id, 'comments_count': comments_count, 'created_at': created_at, 'reposts_count': reposts_count, 'attitudes_count':attitudes_count, 'weibo_content': text}
                                id = mblog.get('id')    # 微博内容id
                                mid = mblog.get('mid')    # 微博内容mid
                                weibo_report_info = weibo_report_content(mid, proxies, lock, thread_name)  # 获取微博用户发表的微博后相关评论信息接口
                                weibo_info.update(weibo_report_info)
                                weibo_attitude_info = weibo_attitude_content(id, proxies, lock, thread_name)  # 获取微博用户发表的微博后相关点赞信息接口
                                weibo_info.update(weibo_attitude_info)
                                weibo_content = json.dumps(weibo_info, ensure_ascii=False)
                                file_write(weibo_content, lock, fileout)

            else:
                print '微博用户发表微博内容的data为空：{weibo_user_id}。'.format(weibo_user_id=weibo_user_id)

        except ReadTimeout as e:
            print 'ReadTimeout异常weibo_user_content：', weibo_user_id

        except ValueError as e:
            print 'ValueError', weibo_user_id

        except WeiboException as e:
            with lock:
                print 'WeiboException异常weibo_user_content：', e.message
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectTimeout as e:
            with lock:
                print 'ConnectTimeout异常weibo_user_content：', e.message
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量weibo_user_content：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies

        except ConnectionError as e:
            with lock:
                print 'ConnectionError异常weibo_user_content：', e.message
                THREAD_PROXY_MAP.pop(thread_name)
                if PROXY_IP_Q.empty():
                    get_redis_proxy()
                    print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
                proxies = PROXY_IP_Q.get(False)
                THREAD_PROXY_MAP[thread_name] = proxies


def weibo_report_content(mid, proxies, lock, thread_name):
    try:
        report_info_ls = []
        weibo_report_url_demo = 'https://m.weibo.cn/comments/hotflow?mid={mid}'
        weibo_report_url = weibo_report_url_demo.format(mid=mid)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '微博用户的微博评论url：', weibo_report_url
        response = requests.get(url=weibo_report_url, proxies=proxies, headers=headers, timeout=10)
        if response.status_code == 418:  # 针对该关键词没有微博内容的现象（服务器返回418状态码）
            print '该用户返回状态码为418：', mid
            raise WeiboException('status code is 418')
        response_json = response.json()
        status = response_json.get('ok')
        if status == 1:
            data = response_json.get('data')
            if data:
                data = data.get('data')
                if data:
                    for report_c in data:
                        user = report_c.get('user')
                        id = user.get('id')    # 评论者的id
                        # print '评论者的id：', id
                        screen_name = user.get('screen_name')    # 评论者昵称
                        # print '评论者昵称：', screen_name
                        report_info = {'id': id, 'screen_name': screen_name}
                        report_info_ls.append(report_info)
        else:
            print '微博内容评论的data为空：{mid}。'.format(mid=mid)
    except ReadTimeout as e:
        print 'ReadTimeout异常weibo_report_content：', mid

    except ValueError as e:
        print 'ValueError',

    except WeiboException as e:
        with lock:
            print 'WeiboException异常weibo_report_content：', e.message
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            THREAD_PROXY_MAP[thread_name] = proxies

    except ConnectTimeout as e:
        with lock:
            print 'ConnectTimeout异常weibo_report_content：', e.message
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量weibo_report_content：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            THREAD_PROXY_MAP[thread_name] = proxies

    except ConnectionError as e:
        with lock:
            print 'ConnectionError异常weibo_report_content：', e.message
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            THREAD_PROXY_MAP[thread_name] = proxies
    weibo_report_info = {'weibo_report': report_info_ls}
    return weibo_report_info


def weibo_attitude_content(id, proxies, lock, thread_name):
    '''
    微博用户发表的内容的相关点赞信息
    :return:
    '''
    try:
        attitude_info_ls = []
        weibo_attitude_url_demo = 'https://m.weibo.cn/api/attitudes/show?id={id}&page=1'
        weibo_attitude_url = weibo_attitude_url_demo.format(id=id)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '微博用户的微博点赞url：', weibo_attitude_url
        response = requests.get(url=weibo_attitude_url, proxies=proxies, headers=headers, timeout=10)
        if response.status_code == 418:  # 针对该关键词没有微博内容的现象（服务器返回418状态码）
            print '该用户返回状态码为418：', id
            raise WeiboException('status code is 418')
        response_json = response.json()
        status = response_json.get('ok')
        if status == 1:
            data = response_json.get('data')
            if data:
                data = data.get('data')
                if data:
                    for attitude_c in data:
                        source = attitude_c.get('source')    # 点赞者的手机型号
                        # print '手机型号：', source
                        user = attitude_c.get('user')
                        id = user.get('id')    # 点赞者的用户id
                        # print '点赞者的用户id：', id
                        screen_name = user.get('screen_name')    # 点赞者的昵称
                        # print '点赞者的昵称：', screen_name
                        attitude_info = {'source': source, 'id': id, 'screen_name': screen_name}
                        attitude_info_ls.append(attitude_info)
        else:
            print '微博内容点赞的data为空：{id}。'.format(id=id)
    except ReadTimeout as e:
        print 'ReadTimeout异常weibo_attitude_content：', id

    except ValueError as e:
        print 'ValueError',

    except WeiboException as e:
        with lock:
            print 'WeiboException异常weibo_attitude_content：', e.message
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            THREAD_PROXY_MAP[thread_name] = proxies

    except ConnectTimeout as e:
        with lock:
            print 'ConnectTimeout异常weibo_attitude_content：', e.message
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量weibo_attitude_content：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            THREAD_PROXY_MAP[thread_name] = proxies

    except ConnectionError as e:
        with lock:
            print 'ConnectionError异常weibo_attitude_content：', e.message
            THREAD_PROXY_MAP.pop(thread_name)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            THREAD_PROXY_MAP[thread_name] = proxies
    weibo_attitude_info = {'weibo_attitude': attitude_info_ls}
    return weibo_attitude_info


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
    dest_file_name = os.path.join(dest_path, 'weibo_user_content_' + file_time)
    tmp_file_name = os.path.join(dest_path, 'weibo_user_content_' + file_time + '.tmp')
    fileout = open(tmp_file_name, 'a')

    threads = []
    for i in range(50):
        t = threading.Thread(target=weibo_user_content_num, args=(lock, fileout))
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

