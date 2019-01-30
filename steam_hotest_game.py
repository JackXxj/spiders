# coding:utf-8
__author__ = 'xxj'

import requests
import time
import os
import re
import Queue
import threading
import lxml.etree
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
}


def get(url, headers, count):
    for i in xrange(count):
        response = r(url, headers, i)
        if response is None:    # 异常
            pass
        elif response.status_code == 200:
            return response
    return None


def r(url, headers, i):
    try:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'url：', url, 'count：', i
        response = requests.get(url=url, headers=headers, timeout=10)
    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException', 'url：', url, '异常类型：', e.message
        response = None
        time.sleep(2)
    return response


def steam_index_parse(fileout):
    url = 'https://store.steampowered.com/stats/'
    # response = requests.get(url=url, headers=headers, timeout=10)
    response = get(url, headers, 10)
    # print response.status_code
    # print response.text
    response_obj = lxml.etree.HTML(response.text)
    game_ls = response_obj.xpath('//tr[@class="player_count_row"]')
    for game in game_ls:
        current_user_number = game.xpath('./td[@align="right"]/span/text()')
        if current_user_number:
            current_user_number = current_user_number[0].replace(',', '').strip()    # 当前玩家人数
        else:
            current_user_number = ''
        most_user_number = game.xpath('./td[@align="right"]/span/text()')
        if most_user_number:
            most_user_number = most_user_number[1].replace(',', '').strip()    # 今日峰值
        else:
            most_user_number = ''
        game_name = game.xpath('./td/a[@class="gameLink"]/text()')
        if game_name:
            game_name = game_name[0].strip()    # 游戏名称
        else:
            game_name = ''
        detail_page_url = game.xpath('./td/a[@class="gameLink"]/@href')
        if detail_page_url:
            detail_page_url = detail_page_url[0]    # 详情页url
        else:
            detail_page_url = ''
        # print current_user_number, most_user_number, game_name, detail_page_url
        description, publish_time, developer, publisher = steam_detail_parse(detail_page_url)
        # 字段：当前玩家人数、今日峰值、游戏名称、游戏介绍、发行日期、开发商、发行商
        content = '\t'.join([current_user_number, most_user_number, game_name, description, publish_time, developer, publisher])
        fileout.write(content)
        fileout.write('\n')
        fileout.flush()


def steam_detail_parse(detail_page_url):
    headers = {
        'Cookie': 'birthtime=504892801;Steam_Language=schinese',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    # response = requests.get(url=detail_page_url, headers=headers, timeout=10)
    response = get(detail_page_url, headers, 5)
    # print response.text
    if response is not None:
        response_obj = lxml.etree.HTML(response.text)
        description = response_obj.xpath('//div[@class="game_description_snippet"]/text()')
        if description:
            description = description[0].replace('\t', '').replace('\r', '').replace('\n', '').strip()   # 游戏介绍
        else:
            description = ''
        publish_time = response_obj.xpath('//div[@class="release_date"]/div[@class="date"]/text()')
        if publish_time:
            publish_time = publish_time[0].strip()    # 发行日期
        else:
            publish_time = ''
        developer = response_obj.xpath('//div[@id="developers_list"]/a/text()')
        if developer:
            developer = developer[0]    # 开发商
        else:
            developer = ''
        publisher = response_obj.xpath('//div[@class="summary column"]/a/text()')
        if publisher:
            publisher = publisher[-1]    # 发行商
        else:
            publisher = ''
        # print '发行日期：', publish_time
        # publish_time = time.strftime('%Y年%m月%d日', time.strptime(publish_time, '%d %b, %Y'))    # 25 Mar, 2013
        # print '新格式化的发行日期：', publish_time
        # print '开发商：', developer
        # print '发行商：', publisher
        # print '游戏介绍：', description
        return description, publish_time, developer, publisher
    else:
        description = ''
        publish_time = ''
        developer = ''
        publisher = ''
        return description, publish_time, developer, publisher


def main():
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    date = time.strftime('%Y%m%d')
    dest_path = '/ftp_samba/112/spider/fanyule_two/steam/'  # linux上的文件目录
    # dest_path = os.getcwd()    # windows上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'steam_hotest_game_' + date)
    tmp_file_name = os.path.join(dest_path, 'steam_hotest_game_' + date + '.tmp')
    fileout = open(tmp_file_name, 'w')
    steam_index_parse(fileout)
    fileout.flush()
    fileout.close()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    start_time = time.time()
    main()
    end_time = time.time()
    print '花费的时间：', end_time - start_time
