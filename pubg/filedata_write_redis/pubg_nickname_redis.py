# coding:utf-8
__author__ = 'xxj'

from rediscluster import StrictRedisCluster
import os
import datetime
import time


def file_write_redis():
    '''
    将源文件中的内容写入redis中
    :return:
    '''
    # wrd_keyword_file_path = file_exists()    # 数据源文件路径
    pubg_nickname_file_path = r'/ftp_samba/112/file_4spider/pubg_nickname/pubg_nickname'    # 数据源文件路径
    startup_nodes = [{'host': 'redis2', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)

    pubg_nickname_file = open(pubg_nickname_file_path, 'r')
    pubg_friends_nickname_length = r.llen('spider:python:pubg_friends:keyword')
    print 'redis中pubg_friends_nickname列表长度：', pubg_friends_nickname_length
    if pubg_friends_nickname_length < 200000:
        for line in pubg_nickname_file:
            r.rpush('spider:python:pubg_friends:keyword', line.strip())
        pubg_friends_nickname_length = r.llen('spider:python:pubg_friends:keyword')
        print '重新写入后redis中pubg_friends_nickname列表长度：', pubg_friends_nickname_length

    pubg_nickname_file = open(pubg_nickname_file_path, 'r')
    pubg_match_nickname_length = r.llen('spider:python:pubg_match:keyword')
    print 'redis中pubg_match_nickname列表长度：', pubg_match_nickname_length
    if pubg_match_nickname_length < 200000:
        for line in pubg_nickname_file:
            r.rpush('spider:python:pubg_match:keyword', line.strip())
        pubg_match_nickname_length = r.llen('spider:python:pubg_match:keyword')
        print '重新写入后redis中pubg_match_nickname列表长度：', pubg_match_nickname_length

    pubg_nickname_file = open(pubg_nickname_file_path, 'r')
    pubg_death_nickname_length = r.llen('spider:python:pubg_death:keyword')
    print 'redis中pubg_death_nickname列表长度：', pubg_death_nickname_length
    if pubg_death_nickname_length < 200000:
        for line in pubg_nickname_file:
            r.rpush('spider:python:pubg_death:keyword', line.strip())
        pubg_death_nickname_length = r.llen('spider:python:pubg_death:keyword')
        print '重新写入后redis中pubg_death_nickname列表长度：', pubg_death_nickname_length


def file_exists():
    '''
    当文件不存在时，获取已有最新的源文件
    :return:
    '''
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')
    wrd_keyword_file_dir = r'/ftp_samba/112/file_4spider/wzs_keyword/'    # 文件目录
    wrd_keyword_file_name = 'wzs_keyword_{current_time}_1.txt'.format(current_time=date)
    wrd_keyword_file_path = os.path.join(wrd_keyword_file_dir, wrd_keyword_file_name)    # 线上源文件路径
    print '源文件路径：', wrd_keyword_file_path
    if not os.path.exists(wrd_keyword_file_path):    # 当前文件不存在，获取前一天的最后一个文件
        wrd_keyword_file_name = os.listdir(wrd_keyword_file_dir)[-1]
        wrd_keyword_file_path = os.path.join(wrd_keyword_file_dir, wrd_keyword_file_name)
        print '最新的来源文件路径：', wrd_keyword_file_path
    return wrd_keyword_file_path


def main():
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    start_time = time.time()
    file_write_redis()
    # file_exists()
    end_time = time.time()
    print '将文本中的关键词导入redis中花费的时间：', end_time-start_time


if __name__ == '__main__':
    main()
