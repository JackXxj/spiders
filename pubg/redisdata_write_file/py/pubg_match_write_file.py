# coding:utf-8
__author__ = 'xxj'

import os
import time
import sys
import threading
from rediscluster import StrictRedisCluster
from rediscluster.exceptions import RedisClusterException

reload(sys)
sys.setdefaultencoding('utf-8')


def pubg_match(lock, r, first_hour, fileout):
    while True:
        try:
            current_hour = time.strftime('%H')
            if current_hour != first_hour:  # 退出进程
                print time.strftime('[%Y-%m-%d %H:%M:%S]：pubg_match()退出')
                return False
            if time.strftime('%M%S') == '0000':
                print time.strftime('当前时间：[%Y-%m-%d %H:%M:%S]')
            data = r.lpop('spider:python:pubg_match:keyword:dest')
            if data:
                with lock:
                    fileout.write(data)
                    fileout.write('\n')
                    fileout.flush()
        except RedisClusterException as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]：RedisClusterException异常')


def main():
    startup_nodes = [{'host': 'redis3', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    pubg_match_length = r.llen('spider:python:pubg_match:keyword:dest')  # redis的数据量
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中pubg_match的数据量：', pubg_match_length
    lock = threading.Lock()
    first_hour = time.strftime('%H')
    date = time.strftime('%Y%m%d')

    dest_path = '/ftp_samba/112/spider/python/pubg'  # linux上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'pubg_match_' + date)
    fileout = open(dest_file_name, 'a')

    threads = []
    for i in xrange(1):
        t = threading.Thread(target=pubg_match, args=(lock, r, first_hour, fileout))
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


if __name__ == '__main__':
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    main()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
