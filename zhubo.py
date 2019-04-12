# coding:utf-8
__author__ = 'xxj'

import requests
import Queue
import re
import time
import json
import os
import datetime
import threading
from threading import Lock, Thread
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
import math
import lxml.etree
import sys
from exceptions import ValueError
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf8')
headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, sdch',
    'accept-language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36'
}
CHINAZ_DETAIL_URL_QUEUE = Queue.Queue()  # chinaz详情页的队列


def zhubo_detail(fileout, lock):
    while not CHINAZ_DETAIL_URL_QUEUE.empty():
        try:
            chinaz_detail_url = CHINAZ_DETAIL_URL_QUEUE.get(False)  # chinaz详情页url
            print 'chinaz主播详情页url：{}'.format(chinaz_detail_url)
            response = requests.get(chinaz_detail_url, headers=headers, timeout=10)
            if response.status_code == 200:
                response_text = response.text
                soup = BeautifulSoup(response_text, 'lxml')

                name = soup.select('div.right_title > p')
                if name:
                    name = name[0].text.replace(u'\xa0', '|er|').replace('\r', '').replace('\n', '')  # 主播名称
                    # print '主播名称：', name
                else:
                    print 'chinaz主播详情页无效：{}'.format(chinaz_detail_url)
                    continue

                zhubo_live_url = soup.select('a[rel="nofollow"]')[0].attrs['href'].replace('\r', '').replace('\n', '')  # 主播直播间url
                # print '直播间url：', zhubo_live_url

                trs = soup.select('div.linear_right > table tr')  # 主播相关类型排名
                if len(trs) == 2:
                    ths = trs[0].select('th')
                    th_text_list = [th.text for th in ths]
                    tds = trs[1].select('td')
                    td_text_list = [td.text for td in tds]
                    # zhubo_rank_dict = dict(zip(td_text_list, th_text_list))
                    zhubo_rank = {'zhubo_rank_platform': td_text_list, 'zhubo_rank_num': th_text_list}
                    # print 'zhubo_rank：', zhubo_rank
                else:
                    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '主播相关类型信息结构分析：{}'.format(chinaz_detail_url)

                # 观众指数
                audience_zhishus = soup.select('ul.liquid li')
                if len(audience_zhishus) == 3:
                    highest_online = audience_zhishus[0].select('div.text b')[0].text  # 最近最高在线
                    # print '最近最高在线：', highest_online
                    average_online = audience_zhishus[1].select('div.text b')[0].text  # 平均在线人数
                    # print '平均在线人数：', average_online
                    total_ratings = audience_zhishus[2].select('div.text b')[0].text  # 累计收视人数
                    # print '累计收视人数：', total_ratings
                    audience_zhishus_dict = {'highest_online': highest_online, 'average_online': average_online,
                                             'total_ratings': total_ratings}
                    # print '观众指数：',audience_zhishus_dict
                else:
                    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '观众指数的结构分析：{}'.format(chinaz_detail_url)

                # 观众指数中最高在线人数趋势图
                hight_online_s = r'var maxline_xAxis = (.*?);'
                pattern_obj = re.compile(hight_online_s, re.S)  # 观众指数中最高在线人数趋势图的时间和值正则提取
                search_obj = pattern_obj.search(response_text)
                if search_obj:
                    hight_online_content = search_obj.group()  # 截取内容再一次提取
                    # print 'hight_online_content：', hight_online_content
                    # 日期提取
                    hight_online_day_s = r'\d{4}-\d{2}-\d{2}'
                    hight_online_day = re.findall(hight_online_day_s, hight_online_content, re.S)  # 观众指数中最高在线人数趋势图的时间正则提取
                    if hight_online_day:
                        hight_online_day = hight_online_day
                        # print '最高在线人数趋势图的时间：', hight_online_day
                    else:
                        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '最高在线人数趋势图中时间正则匹配分析：{}'.format(chinaz_detail_url)
                    # 日期对应的数值提取
                    hight_online_num_s = r'\d+,'
                    hight_online_num = re.findall(hight_online_num_s, hight_online_content, re.S)
                    if hight_online_num:
                        hight_online_num = [num.replace(',', '') for num in hight_online_num]
                        # print '最高在线人数趋势图的数值：', hight_online_num
                    else:
                        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '最高在线人数趋势图中数值正则匹配分析：{}'.format(chinaz_detail_url)
                    # highest_online_dict = dict(zip(hight_online_day, hight_online_num))
                    highest_online_dict = {'hight_online_day': hight_online_day, 'hight_online_num': hight_online_num}
                    # highest_online_dict1 = {'highest_online': highest_online_dict}  # 观众指数中最高在线人数趋势图信息
                    # print '最高在线人数趋势图：', highest_online_dict
                else:
                    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '最高在线人数趋势图正则匹配分析：{}'.format(chinaz_detail_url)

                # 主播魅力指数
                # 主播魅力指数类型提取
                charm_zhishu_type_s = r'var charmP_legend = (.*?)],'
                search_obj = re.search(charm_zhishu_type_s, response_text, re.S)
                if search_obj:
                    charm_zhishu_type_content = search_obj.group()
                    # print charm_zhishu_type_content
                    charm_zhishu_type_s1 = r'\'(\W+?)\''
                    charm_zhishu_type = re.findall(charm_zhishu_type_s1, charm_zhishu_type_content, re.S)
                    if charm_zhishu_type:
                        charm_zhishu_type = charm_zhishu_type
                        # print '主播魅力指数的类型：', charm_zhishu_type
                    else:
                        time.strftime('[%Y-%m-%d %H:%M:%S]'), '主播魅力指数类型正则匹配分析：{}'.format(chinaz_detail_url)
                else:
                    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '主播魅力指数类型正则匹配分析：{}'.format(chinaz_detail_url)

                # 主播魅力指数数值提取
                charm_zhishu_num_s = r'charmP_data = (.*?)],'
                search_obj = re.search(charm_zhishu_num_s, response_text, re.S)
                if search_obj:
                    charm_zhishu_num_content = search_obj.group()
                    # print charm_zhishu_num_content
                    charm_zhishu_num_s1 = r'\d+'
                    charm_zhishu_num = re.findall(charm_zhishu_num_s1, charm_zhishu_num_content, re.S)
                    if charm_zhishu_num:
                        charm_zhishu_num = charm_zhishu_num
                        # print '主播魅力指数的数值：', charm_zhishu_num
                    else:
                        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '主播魅力指数数值正则匹配分析：{}'.format(chinaz_detail_url)
                else:
                    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '主播魅力指数数值正则匹配分析：{}'.format(chinaz_detail_url)

                # charm_zhishu_dict = dict(zip(charm_zhishu_type, charm_zhishu_num))
                charm_zhishu_dict = {'charm_zhishu_type': charm_zhishu_type, 'charm_zhishu_num': charm_zhishu_num}
                # charm_zhishu_dict1 = {'charm_zhishu': charm_zhishu_dict}  # 主播魅力指数信息
                # print '主播魅力指数：', charm_zhishu_dict

                # 影响力
                weixin = soup.select('div.weixin b')[0].text  # 微信值
                weibo = soup.select('div.weibo b')[0].text  # 微博值
                baidu = soup.select('div.baidu b')[0].text  # 百度值
                toutiao = soup.select('div.toutiao b')[0].text  # 头条
                # print '微信值：', weixin
                # print '微博值：', weibo
                # print '百度值：', baidu
                # print '头条值：', toutiao
                influence_dict = {'weixin': weixin, 'weibo': weibo, 'baidu': baidu, 'toutiao': toutiao}
                # influence_dict1 = {'influence_zhishu': influence_dict}  # 影响力信息
                # print '影响力：', influence_dict
                # zhubo_content.update(influence_dict1)

                # 潜力指数
                potential_zhishu_s = r'var potential_data = (.*?);'
                pattern_obj = re.compile(potential_zhishu_s, re.S)  # 潜力指数趋势图的时间和值正则提取
                search_obj = pattern_obj.search(response_text)
                if search_obj:
                    potential_zhishu_content = search_obj.group()  # 截取内容再一次提取
                    # print 'potential_zhishu_content：', potential_zhishu_content
                    # 日期提取
                    potential_zhishu_week_s = r'\'(\W+?)\''
                    potential_zhishu_week = re.findall(potential_zhishu_week_s, potential_zhishu_content,
                                                       re.S)  # 潜力指数趋势图的时间正则提取
                    if potential_zhishu_week:
                        potential_zhishu_week = potential_zhishu_week
                        # print '潜力指数趋势图的时间：', potential_zhishu_week
                    else:
                        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '潜力指数趋势图中时间正则匹配分析：{}'.format(chinaz_detail_url)
                    # 日期对应的数值提取
                    potential_zhishu_num_s = r'\d+,'
                    potential_zhishu_num = re.findall(potential_zhishu_num_s, potential_zhishu_content, re.S)
                    if potential_zhishu_num:
                        potential_zhishu_num = [num.replace(',', '') for num in potential_zhishu_num]
                        # print '潜力指数趋势图的数值：', potential_zhishu_num
                    else:
                        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '潜力指数趋势图中值正则匹配分析：{}'.format(chinaz_detail_url)
                    # potential_zhishu_dict = dict(zip(potential_zhishu_week, potential_zhishu_num))
                    potential_zhishu_dict = {'potential_zhishu_week': potential_zhishu_week,
                                             'potential_zhishu_num': potential_zhishu_num}
                    # potential_zhishu_dict1 = {'potential_zhishu': potential_zhishu_dict}  # 潜力指数信息
                    # print '潜力指数：', potential_zhishu_dict
                    # zhubo_content.update(potential_zhishu_dict1)
                else:
                    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '潜力指数趋势图正则匹配分析：{}'.format(chinaz_detail_url)

                # chinaz详情页获取到的相关信息
                zhubo_content = {'name': name, 'zhubo_live_url': zhubo_live_url, 'zhubo_rank': zhubo_rank,
                                 'audience_zhishu': audience_zhishus_dict, 'highest_online': highest_online_dict,
                                 'charm_zhishu': charm_zhishu_dict, 'influence_zhishu': influence_dict,
                                 'potential_zhishu': potential_zhishu_dict
                                 }

                search_obj = re.search(r'.*?zhubo/(.*?)/', chinaz_detail_url, re.S)  # 从chinaz详情页中提取主播平台字段
                if search_obj:
                    plantform = search_obj.group(1)
                    print '主播平台：', plantform
                    plantform_dict = {'douyu': douyu, 'huya': huya, 'chushou': chushou, 'bilibili': bilibili,
                                      'panda': panda,
                                      'huajiao': huajiao, 'fangxin': fangxin, 'longzhu': longzhu, '9xiu': jiuxiu,
                                      'qf56': qf56,
                                      'renren': renren, 'inke': inke, 'KK': kk, 'meipai': meipai, 'CC': CC}
                    plantform_def = plantform_dict.get(plantform)
                    if plantform_def is not None:
                        try:
                            zhubo_info = plantform_def(zhubo_live_url)  # 直播平台主播直播间信息接口
                        except BaseException as e:
                            print '主播直播间信息获取异常：{}'.format(chinaz_detail_url), e.message
                            zhubo_info = {}
                        if zhubo_info is None:    # 1、主播直播接口返回None；2、异常返回的None
                            print '主播直播间的粉丝数和观看人数为None：', zhubo_live_url
                            zhubo_info = {}

                        zhubo_content.update(zhubo_info)
                else:
                    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'chinaz主播平台提取失败，分析url：{}'.format(chinaz_detail_url)

                zhubo_content_str = json.dumps(zhubo_content, ensure_ascii=False)
                data_write_file(zhubo_content_str, fileout, lock)    # 写入文本的接口

            else:
                print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'chinaz主播详情页的response_code is not 200', chinaz_detail_url

        except ReadTimeout as e:
            with lock:
                print 'ReadTimeout异常：', chinaz_detail_url
                CHINAZ_DETAIL_URL_QUEUE.put(chinaz_detail_url)

        except ConnectTimeout as e:
            with lock:
                print 'ConnectTimeout异常：', chinaz_detail_url
                CHINAZ_DETAIL_URL_QUEUE.put(chinaz_detail_url)

        except ConnectionError as e:
            with lock:
                print 'ConnectionError异常：', chinaz_detail_url
                CHINAZ_DETAIL_URL_QUEUE.put(chinaz_detail_url)

        except BaseException as e:
            print 'BaseException异常：', e.message


def data_write_file(zhubo_content_str, fileout, lock):
    with lock:
        fileout.write(zhubo_content_str)
        fileout.write('\n')
        fileout.flush()


def douyu(zhubo_live_url):
    '''
    斗鱼主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # detail_url = 'https://www.douyu.com/2082749?ptag=livechinaz'
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_obj = lxml.etree.HTML(response.text)
        tags = response_obj.xpath('//div[contains(@class, "Title-categoryList")]/a/text()')
        tag = '|er|'.join(tags)
        print '标签：', tag

    else:
        tag = ''
        print '斗鱼主播的response_code is not 200：{}'.format(zhubo_live_url)

    search_obj = re.search(r'\d+', zhubo_live_url, re.S)
    if search_obj:
        room_id = search_obj.group()
        # print '斗鱼主播的room_id：', room_id
    else:
        print '斗鱼主播提取room_id失败，分析斗鱼主播直播间url：{}'.format(zhubo_live_url)
        return None
    detail_url = 'https://www.douyu.com/swf_api/h5room/{room_id}'.format(room_id=room_id)
    # response = get(detail_url, 3, '斗鱼主播直播间信息详情页url：')
    print '斗鱼主播直播间信息详情页url：{}'.format(detail_url)
    response = requests.get(detail_url, headers=headers, timeout=10)
    if response.status_code == 200:
        try:
            response_json = response.json()
        except ValueError as e:
            return None
        data = response_json.get('data')
        if data:
            online = data.get('online')  # 在线观看人数
            print '斗鱼在线观看人数：', online
            fans = data.get('fans')  # 粉丝数量
            print '斗鱼关注人数：', fans
            owner_avatar = data.get('owner_avatar')  # 头像url
            print '头像url：', owner_avatar
            return {'online_num': online, 'fans_num': fans, 'owner_avatar': owner_avatar, 'tags': tag}
        else:    # 存在开发接口无法获取的情况(通过主播鱼吧接口获取)
            room_id_href = response_obj.xpath('//a[@class="Title-anchorName"]/@href')[0]
            room_id_obj = re.search(r'\d+', room_id_href, re.S)
            room_id = room_id_obj.group()
            print 'room_id：', room_id

            fans_num, owner_avatar = douyu_fans_num(room_id)  # 获取主播的粉丝数接口
            print '粉丝数：', fans_num
            print '主播头像url：', owner_avatar
            return {'online_num': 0, 'fans_num': fans_num, 'owner_avatar': owner_avatar, 'tags': tag}
    else:
        print '斗鱼主播的response_code is not 200：{}'.format(detail_url)
        return None


def douyu_fans_num(room_id):
    url = 'https://www.douyu.com/betard/' + room_id
    print '获取主播鱼吧url的参数的url：{}'.format(url)
    response = requests.get(url=url, headers=headers, timeout=10)
    response_json = response.json()
    room = response_json.get('room')
    if room:
        up_id = room.get('up_id')
        if up_id:
            print 'up_id：', up_id
            fans_num, owner_avatar = douyu_yuba(up_id)     # 通过鱼吧获取粉丝数接口
            return fans_num, owner_avatar
        else:
            return None
    else:
        return None


def douyu_yuba(up_id):
    url = 'https://yuba.douyu.com/wbapi/web/user/detail/' + up_id
    response = requests.get(url=url, headers=headers, timeout=10)
    response_json = response.json()
    data = response_json.get('data')
    if data:
        fans_num = data.get('fans_num')
        owner_avatar = data.get('avatar')
        return fans_num, owner_avatar
    else:
        return None, None


def huya(zhubo_live_url):
    '''
    虎牙主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # detail_url = 'https://www.huya.com/1106460601?ptag=livechinaz'
    # response = get(zhubo_live_url, 3, '虎牙主播直播间信息详情页url：')
    print '虎牙主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        tip = soup.select('p.error-tip > a')  # 跳转链接
        if tip:
            detail_url = tip[0].text
            print '虎牙主播房间链接变更为：{}'.format(detail_url)
            response = requests.get(detail_url, headers=headers, timeout=10)
            if response.status_code != 200:
                print '虎牙主播变更后的直播间的response_code is not 200'
                return None
            else:
                soup = BeautifulSoup(response.text, 'lxml')
        response_obj = lxml.etree.HTML(response.text)
        owner_avatar = soup.select('img#avatar-img')[0].attrs.get('src')  # 头像url
        print '头像url：', owner_avatar
        a_tags = response_obj.xpath('//span[@class="host-channel"]/a')
        tags = [a_tag.xpath('string(.)').strip() for a_tag in a_tags]
        tag = '|er|'.join(tags)
        print '标签：', tag
        live_count = soup.select('em#live-count')  # 观看人数
        if live_count:
            live_count = live_count[0].text.replace(',', '')
        else:
            live_count = 0
        print '虎牙观看人数：', live_count

        to_key = soup.select('div.mod-sidebar')  # 获取订阅人数url的参数
        if to_key:
            to_key = to_key[0].attrs.get('data-source')    # 获取data-source的参数值
        else:    # 无法获取fans
            return None

        activity_count = huya_fans(to_key)    # 虎牙实时粉丝数量接口
        if activity_count:
            print '虎牙订阅人数：', activity_count
            return {'online_num': live_count, 'fans_num': activity_count, 'owner_avatar': owner_avatar, 'tags': tag}
        else:
            print '获取到的虎牙订阅人数为无效数据'
            return None
    else:
        print '虎牙主播的response_code is not 200'
        return None


def huya_fans(to_key):
    url = 'https://api.huya.com/subscribe/getSubscribeStatus?to_key={to_key}&to_type=2'.format(to_key=to_key)
    print '获取虎牙主播的粉丝数量url：{}'.format(url)
    response = requests.get(url=url, headers=headers, timeout=10)
    response_json = response.json()
    subscribe_count = response_json.get('subscribe_count')
    print '粉丝数量：', subscribe_count
    return subscribe_count


def chushou(zhubo_live_url):
    '''
    触手主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # zhubo_live_url = 'https://chushou.tv/room/7726024.htm?ptag=livechinaz'
    # response = get(zhubo_live_url, 3, '触手主播直播间信息详情页url：')
    print '触手主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        owner_avatar = soup.select('div.zb_player_face  img')  # 头像url
        if owner_avatar:
            owner_avatar = owner_avatar[0].attrs.get('src')
            if 'http:' not in owner_avatar:
                owner_avatar = 'http:{}'.format(owner_avatar)
        else:
            owner_avatar = ''
        print '触手头像url：', owner_avatar
        live_count = soup.select('span.onlineCount')  # 在线观看人数
        if live_count:
            live_count = live_count[0].text
        else:
            live_count = 0
        print '触手观看人数：', live_count
        activity_count = soup.select('div.zb_attention_left')[0].attrs['data-subscribercount']  # 关注人数
        # if activity_count:
        #     activity_count = activity_count[0].attrs['data-subscribercount']
        # else:
        #     activity_count = ''
        print '触手关注人数：', activity_count
        return {'online_num': live_count, 'fans_num': activity_count, 'owner_avatar': owner_avatar, 'tags': ''}
    else:
        print '触手的response_code is not 200'
        return None


def bilibili(zhubo_live_url):
    '''
    bilibili主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # zhubo_live_url = 'https://live.bilibili.com/11630995?ptag=livechinaz'
    # response = get(zhubo_live_url, 3, 'bilibili主播直播间信息详情页url：')
    search_obj = re.search(r'\d+', zhubo_live_url, re.S)
    if search_obj:
        room_id = search_obj.group()
        print 'bilibili主播的room_id：', room_id
        avatar_url = 'https://api.live.bilibili.com/live_user/v1/UserInfo/get_anchor_in_room?roomid={}'.format(room_id)
        response = requests.get(avatar_url, headers=headers, timeout=10)
        if response.status_code == 200:
            response_json = response.json()
            owner_avatar = response_json.get('data').get('info').get('face')
            print 'face：', owner_avatar
        else:
            owner_avatar = ''
            print 'bilibili主播头像的response_code is not 200'
    else:
        print 'bilibili主播提取room_id失败，分析bilibili主播直播间url：{}'.format(zhubo_live_url)
        owner_avatar = ''

    print 'bilibili主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response.encoding = 'utf-8'
        parent_area_name_obj = re.search(r'"parent_area_name":"(.*?)",', response.text, re.S)
        if parent_area_name_obj:
            parent_area_name = parent_area_name_obj.group(1)
            print 'parent_area_name：', parent_area_name
        else:
            parent_area_name = ''
            print '未获取到parent_area_name标签'

        area_name_obj = re.search(r'"area_name":"(.*?)",', response.text, re.S)
        if area_name_obj:
            area_name = area_name_obj.group(1)
            print 'area_name：', area_name
        else:
            area_name = ''
            print '未获取到area_name标签'
        tags = '|er|'.join([parent_area_name, area_name])

        pattern_obj = re.compile(r'.*?<script>window.__NEPTUNE_IS_MY_WAIFU__=(.*?)</script>', re.S)
        search_obj = pattern_obj.search(response.text)
        if search_obj:
            pattern_obj = re.compile(r'.*attention\":(\d+),.*online":(\d+),', re.S)
            search_obj = pattern_obj.search(response.text)
            if search_obj:
                attention = search_obj.group(1)  # 关注人数
                print 'bilibili关注人数：', attention
                online = search_obj.group(2)  # 在线人数
                print 'bilibili在线人数：', online
                return {'online_num': online, 'fans_num': attention, 'owner_avatar': owner_avatar, 'tags': tags}
            else:
                print 'bilibili主播的关注人数和在线观看人数正则提取失败'
                return None
        else:
            print 'bilibili主播直播间正则解析异常'
            return None
    else:
        print 'bilibili主播直播间的response_code is not 200'
        return None


def panda(zhubo_live_url):
    '''
    熊猫主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # detail_url = 'https://www.panda.tv/337852?ptag=livechinaz'    # 主播详情页
    search_obj = re.search(r'\d+', zhubo_live_url, re.S)
    if search_obj:
        room_id = search_obj.group()
    else:
        print '熊猫主播提取room_id失败', zhubo_live_url
        return None

    online = panda_online(room_id)  # 熊猫主播在线观看人数接口
    fans = panda_fans(room_id)  # 熊猫主播粉丝数接口
    if (online is None) or (fans is None):
        return None
    else:
        return {'online_num': online, 'fans_num': fans}


def panda_online(room_id):
    online_url = 'https://www.panda.tv/api_room_v2?roomid={room_id}'.format(room_id=room_id)
    # response = get(online_url, 3, '熊猫主播平台在线观看人数url：')
    print '熊猫主播平台在线观看人数url：{}'.format(online_url)
    response = requests.get(online_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        errno = response_json.get('errno')
        if errno == 0:
            data = response_json.get('data')
            if data:
                person_num = data.get('roominfo').get('person_num')  # 在线观看人数
                print '在线观看人数：', person_num
                return person_num
            else:
                print '熊猫主播的data为空'
                return None
        else:  # 主播已下架
            print '熊猫主播的在线观看人数接口异常：', response_json
            return None
    else:
        print '熊猫主播的response_code is not 200'
        return None


def panda_fans(room_id):
    '''
    熊猫主播粉丝数量信息
    :param room_id:
    :return:
    '''
    fans_url = 'https://www.panda.tv/room_followinfo?token=&roomid={roomid}'.format(roomid=room_id)  # 主播粉丝数量页面
    # response = get(fans_url, 3, '熊猫主播平台粉丝数url：')
    print '熊猫主播平台粉丝数url：{}'.format(fans_url)
    response = requests.get(fans_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        errno = response_json.get('errno')
        if errno == 0:
            data = response_json.get('data')
            if data:
                fans = data.get('fans')
                print '熊猫粉丝数量：', fans
                return fans
            else:
                print '熊猫主播的data为空'
                return None
        else:  # 该平台下的被封
            print '该熊猫主播无粉丝：', response_json
            return None
    else:
        print '熊猫中的response_code is not 200'
        return None


def huajiao(zhubo_live_url):
    '''
    花椒主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # detail_url = 'http://www.huajiao.com/user/21491770?ptag=livechinaz'
    # detail_url = 'http://www.huajiao.com/user/134497601?ptag=livechinaz'
    search_obj = re.search(r'\d+', zhubo_live_url, re.S)
    if search_obj:
        room_id = search_obj.group()
        # print '花椒主播的room_id：', room_id
    else:
        print '花椒主播提取room_id失败：', zhubo_live_url
        return None
    # response = get(zhubo_live_url, 3, '花椒主播直播间信息详情页url：')
    print '花椒主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        owner_avatar = soup.select('div.avatar img')
        if owner_avatar:
            owner_avatar = owner_avatar[0].attrs['src']
        else:
            owner_avatar = ''
        print '主播头像url：', owner_avatar

        tags_obj = soup.select('p.tags span')
        tags = [tag_obj.string for tag_obj in tags_obj]
        tag = '|er|'.join(tags)
        print '标签：', tag

        p_ls = soup.select('div.wrap ul li h4')  # 主播 (从chinaz跳转到花椒有两种界面)
        if len(p_ls) == 4:
            fans = p_ls[0].text.strip()
            print '花椒主播粉丝数量：', fans
        else:
            print '花椒主播粉丝数量页面结构：', zhubo_live_url
            return None

        online = huajiao_online(room_id)  # 花椒主播在线观看人数接口
        if online is None:
            return None
    else:
        print '花椒的response_code is not 200'
        return None
    return {'online_num': online, 'fans_num': fans, 'owner_avatar': owner_avatar, 'tags': tag}


def huajiao_online(room_id):
    online_url = 'http://webh.huajiao.com/User/getUserFeeds?uid={room_id}'.format(room_id=room_id)
    # response = get(online_url, 3, '花椒主播在线观看人数url：')
    print '花椒主播在线观看人数url：{}'.format(online_url)
    response = requests.get(online_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        # print response_json
        data = response_json.get('data')
        if data:
            feed = data.get('feeds')[0].get('feed')
            rtop = feed.get('rtop')  # 是否正在直播
            # print 'rtop：', rtop
            if rtop == u'直播中':
                watches = feed.get('watches')  # 观看人数
            else:
                watches = 0
            print '花椒主播观看人数：', watches
            return watches
        else:
            print '花椒主播在线观看人数的data为空'  # 存在
            return None
    else:
        print 'huajiao_online的response_code is not 200'
        return None


def fangxin(zhubo_live_url):
    '''
    繁星主播直播间信息
    :param detail_url:
    :return:
    '''
    # detail_url = 'http://fanxing.kugou.com/1045241?ptag=livechinaz'
    # detail_url = 'http://fanxing.kugou.com/1028723?ptag=livechinaz'
    search_obj = re.search(r'\d+', zhubo_live_url, re.S)
    if search_obj:
        chinaz_id = search_obj.group()
        # print 'chinaz_id：', chinaz_id
    else:
        print '繁星解析chinaz_id字段', zhubo_live_url
        return None
    # response = get(zhubo_live_url, 3, '繁星主播直播间信息详情页url：')
    print '繁星主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        user_id = soup.select('ul#giftReceiverList li')  # 获取主播id
        if user_id:
            user_id = user_id[0].attrs['_id']
            print '获取繁星主播id：', user_id
            fans, owner_avatar = fangxin_fans_num(user_id)  # 繁星主播粉丝数量接口
            online = fangxin_online_num(chinaz_id, user_id)  # 繁星主播在线观看人数接口
            if (fans is None) or (online is None):
                return None
            else:
                return {'online_num': online, 'fans_num': fans, 'owner_avatar': owner_avatar}
        else:
            print '繁星该主播无主播id', zhubo_live_url
            return None
    else:
        print '繁星中的response_code is not 200'
        return None


def fangxin_fans_num(user_id):
    fans_url_demo = 'https://fx.service.kugou.com/VServices/RoomService.RoomService.getStarInfo/{user_id}'
    fans_url = fans_url_demo.format(user_id=user_id)
    # response = get(fans_url, 3, '繁星主播粉丝数量url：')
    print '繁星主播粉丝数量url：{}'.format(fans_url)
    response = requests.get(fans_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        data = response_json.get('data')
        if data:
            fans_count = data.get('fansCount')  # 粉丝数量
            print '繁星主播粉丝数量：', fans_count
            user_logo = data.get('userLogo')  # 头像url
            print '头像url：', user_logo
            return fans_count, user_logo
        else:
            print '繁星主播粉丝数量的data is None'
            return None, None
    else:
        print '繁星中的response_code is not 200'
        return None, None


def fangxin_online_num(chinaz_id, user_id):
    online_url_demo = 'https://fx.service.kugou.com/gw/api/v1/c/n/richlevel/lightup/viewerlist/{chinaz_id}/{user_id}.jsonp'
    online_url = online_url_demo.format(chinaz_id=chinaz_id, user_id=user_id)
    # response = get(online_url, 3, '繁星主播在线观看人数url：')
    print '繁星主播在线观看人数url：{}'.format(online_url)
    response = requests.get(online_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        data = response_json.get('data')
        if data:
            count = data.get('count')
            print '繁星在线观看人数：', count
        else:
            count = 0  # 需要再次观察是否主播休息时data为空
        return count
    else:
        print '繁星中的response_code is not 200'
        return None


def longzhu(zhubo_live_url):
    '''
    龙珠主播直播间信息
    :param detail_url:
    :return:
    '''
    # response = get(zhubo_live_url, 3, '龙珠主播直播间信息详情页url：')
    print '龙珠主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        # print response.text
        tags_search_obj = re.search(r'var roomTags = (.*?);', response.text, re.S)
        if tags_search_obj:
            tags = tags_search_obj.group(1)
            tags = eval(tags)
            tag = '|er|'.join(tags)
            print '标签：', tag
        else:
            tag = ''

        search_obj = re.search(r'var roomInfo = (.*?)};', response.text, re.S)
        if search_obj:
            content_dict = search_obj.group(1) + '}'
            # response_json = json.loads(search_obj.group(1))
            response_json = json.loads(content_dict)
            room_id = response_json.get('RoomId')
            # print '龙珠的room_id：', room_id
            logo = response_json.get('Logo')
            print '头像url：', logo
            longzhu_con = longzhu_online_fans(room_id, logo, tag)  # 龙珠在线观看人数和粉丝数量接口
            return longzhu_con
        else:
            print '龙珠中的room_id正则提取失败, 重新提取'
            return None
    else:
        print '龙珠中的response_code is not 200'
        return None


def longzhu_online_fans(room_id, logo, tag):
    longzhu_url_demo = 'http://roomapicdn.longzhu.com/room/roomstatus?roomid={roomid}&lzv=1'
    longzhu_url = longzhu_url_demo.format(roomid=room_id)
    # response = get(longzhu_url, 3, '龙珠主播粉丝和在线观看人数url：')
    print '龙珠主播粉丝和在线观看人数url：{}'.format(longzhu_url)
    response = requests.get(longzhu_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        # print response_json
        online = response_json.get('OnlineCount')  # 在线观看人数
        print '龙珠在线观看人数：', online
        fans = response_json.get('RoomSubscribeCount')  # 粉丝数量
        print '龙珠粉丝数量：', fans
        return {'online_num': online, 'fans_num': fans, 'owner_avatar': logo, 'tags': tag}
    else:
        print '龙珠中的观看人数和粉丝数量的response_code is not 200'
        return None


def CC(zhubo_live_url):
    # zhubo_live_url = 'http://cc.163.com/600111/?ptag=livechinaz'
    # response = get(zhubo_live_url, 3, 'CC主播直播间信息详情页url：')
    print 'CC主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        owner_avatar = soup.select('img.js-anchor-portrait')
        if owner_avatar:
            owner_avatar = owner_avatar[0].attrs.get('src')
            print '头像url：', owner_avatar
        else:
            owner_avatar = ''
        fans_num = soup.select('span#follow_num')  # 粉丝数量
        if fans_num:
            fans_num = fans_num[0].text
            print '粉丝数量：', fans_num
            if fans_num == '':
                return None
            else:
                online = 0
                return {'online_num': online, 'fans_num': fans_num, 'owner_avatar': owner_avatar}
        else:
            print '该主播无粉丝元素：'
            return None
    else:
        print 'CC主播的response_code is not 200'
        return None


def qf56(zhubo_live_url):
    '''
    千帆主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # detail_url = 'https://qf.56.com/6037740?ptag=livechinaz'
    # detail_url = 'https://qf.56.com/520004?ptag=livechinaz'
    # response = get(zhubo_live_url, 3, '千帆主播直播间信息详情页url：')
    print '千帆主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        fans = soup.select('div.a_f_count')[0].text.strip()  # 粉丝数量
        print'粉丝数量：', fans

        owner_avatar = soup.select('div.anchor_avatar img')
        if owner_avatar:
            owner_avatar = owner_avatar[0].attrs.get('src')
            print '头像url：', owner_avatar
        else:
            owner_avatar = ''

        search_obj = re.search(r'anchorId: \'(.*?)\',', response.text, re.S)
        if search_obj:
            anchor_id = search_obj.group(1)
            # print '千帆主播的anchor_id：', anchor_id
            online = qf56_online(anchor_id)  # 千帆主播在线观看人数接口
            if online is None:
                online = 0
        else:
            print '该主播anchor_id获取解析：', zhubo_live_url
            return None
    else:
        print '千帆主播的response_code is not 200'
        return None
    return {'online_num': online, 'fans_num': fans, 'owner_avatar': owner_avatar}


def qf56_online(anchor_id):
    online_url = 'https://qf.56.com/room/entranceBusi.do?anchorId={anchorId}'.format(anchorId=anchor_id)
    # response = get(online_url, 3, '千帆主播观看人数url：')
    print '千帆主播观看人数url：{}'.format(online_url)
    response = requests.get(online_url, headers=headers, timeout=10)
    # print response.text
    if response.status_code == 200:
        response_json = response.json()
        online = response_json.get('message').get('auCount')  # 在线观看人数
        print '千帆在线观看人数：', online
        return online

    else:
        print '千帆主播观看人数的response_code is not 200'
        return None


def jiuxiu(zhubo_live_url):
    '''
    九秀主播直播间信息
    :param detail_url:
    :return:
    '''
    # detail_url = 'http://www.9xiu.com/52060020?ptag=livechinaz'
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'lxml')
    owner_avatar = soup.select('div.ahoInfoImg img')
    if owner_avatar:
        owner_avatar = owner_avatar[0].attrs.get('src')
        print '头像url：', owner_avatar
    else:
        owner_avatar = ''

    search_obj = re.search(r'.*com/(\d+)', zhubo_live_url, re.S)
    if search_obj:
        room_id = search_obj.group(1)
        # print 'room_id：', room_id
    else:
        print '九秀解析room_id字段'
        return None
    fans_url = 'http://www.9xiu.com/room/live/initRoom?newsock=1&rid={rid}'.format(rid=room_id)
    # response = get(fans_url, 3, '九秀主播直播间信息详情页url：')
    print '九秀主播直播间信息详情页url：{}'.format(fans_url)
    response = requests.get(fans_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        # print response_json
        data = response_json.get('data')
        if data:
            fans_count = data.get('roomInfo').get('fans_count')  # 粉丝数量
            print '粉丝数量：', fans_count
            return {'fans_num': fans_count, 'owner_avatar': owner_avatar}
        else:
            print '九秀主播的data为空'
            return None
    else:
        print '九秀主播的response_code is not 200'
        return None


def renren(zhubo_live_url):
    '''
    人人主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    detail_url = 'http://zhibo.renren.com/liveroom/2421346?ptag=livechinaz'
    # response = get(zhubo_live_url, 3, '人人主播直播间信息详情页url：')
    print '人人主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        # print response.text
        soup = BeautifulSoup(response.text, 'lxml')
        owner_avatar = soup.select('img.head')
        # print owner_avatar
        if owner_avatar:
            owner_avatar = owner_avatar[-1].attrs.get('src')
            print '头像url：', owner_avatar
        else:
            owner_avatar = ''

        fans_str = r'<li class="like-count"><p class="num">(\d*)'
        search_obj = re.search(fans_str, response.text, re.S)
        if search_obj:
            fans_num = search_obj.group(1)
            print '粉丝数量：', fans_num
        else:  # 无效页面
            print '人人粉丝数量正则匹配的方法：', zhubo_live_url
            return None
        online_str = r'<li class="watch-count"><p class="num">(\d*)'
        search_obj = re.search(online_str, response.text, re.S)
        if search_obj:
            online_num = search_obj.group(1)
            print '在线观看人数：', online_num
        else:  # 无效页面
            print '人人在线观看人数正则匹配方法：', zhubo_live_url
            return None
    else:
        print '人人主播的response_code is not 200'
        return None
    return {'online_num': online_num, 'fans_num': fans_num, 'owner_avatar': owner_avatar}


def inke(zhubo_live_url):
    # detail_url = 'http://www.inke.cn/live.html?uid=12959551&id=1547545727911376&ptag=livechinaz'
    inke_re_s = r'.*uid=(\d*)&id=(\d*)'
    search_obj = re.search(inke_re_s, zhubo_live_url, re.S)
    if search_obj:
        uid = search_obj.group(1)
        id = search_obj.group(2)
        # print 'uid：', uid, 'id：', id
    else:
        print '映客uid和id正则解析失败：', zhubo_live_url
        return None
    online_url = 'http://webapi.busi.inke.cn/web/live_share_pc?uid={uid}&id={id}'.format(uid=uid, id=id)
    # response = get(online_url, 3, '映客主播平台详情页url：')
    print '映客主播平台详情页url：{}'.format(online_url)
    response = requests.get(online_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        data = response_json.get('data')
        if data:
            online_users = data.get('file').get('online_users')  # 在线观众
            print '在线观众：', online_users
            owner_avatar = data.get('file').get('pic')
            print '头像url：', owner_avatar
            return {'online_num': online_users, 'owner_avatar': owner_avatar}
        else:
            print '映客主播的data为空'
            return None
    else:
        print '映客主播的response_code is not 200'
        return None


def kk(zhubo_live_url):
    '''
    KK主播直播间信息
    :param detail_url:
    :return:
    '''
    # response = get(zhubo_live_url, 3, 'KK主播平台详情页url：')
    print 'KK主播平台详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        owner_avatar = soup.select('img.avatar')
        if owner_avatar:
            owner_avatar = owner_avatar[0].attrs.get('src')
            print '头像url：', owner_avatar
        else:
            owner_avatar = ''
        room_id = soup.select('div.line1 span.name')  # 主播房间id
        if room_id:
            room_id = room_id[0].attrs['data-uid']
            # print 'room_id：', room_id
            online = kk_online(room_id)
        else:
            print 'kk中该主播无room_id'
            return None
        fans_num = soup.select('div.focus_panel span.number')
        if fans_num:
            fans_num = fans_num[0].text
            print '粉丝数：', fans_num
        else:
            print '该kk主播无粉丝'
            return None
        return {'online_num': online, 'fans_num': fans_num, 'owner_avatar': owner_avatar}
    else:
        print 'KK的response_code is not 200'
        return None


def kk_online(room_id):
    kk_online_url = 'https://www.kktv5.com/CDN/output/M/10/I/10002033/P/userIds-{room_id}/json.js'.format(room_id=room_id)
    # response = get(kk_online_url, 3, 'KK主播在线观看人数详情页url：')
    print 'KK主播在线观看人数详情页url：{}'.format(kk_online_url)
    response = requests.get(kk_online_url, headers=headers, timeout=10)
    if response.status_code == 200:
        response_json = response.json()
        roomList = response_json.get('roomList')
        if roomList:
            online = roomList[0].get('onlineCount')
            print '在线观看人数数量：', online
            return online
        else:
            print 'kk主播在线观看人数列表为空', response_json
            return None
    else:
        print 'kk主播的response_code is not 200'
        return None


def meipai(zhubo_live_url):
    '''
    美拍主播直播间信息
    :param zhubo_live_url:
    :return:
    '''
    # detail_url = 'https://www.meipai.com/media/1076681696?client_id=1089857306'
    # response = get(zhubo_live_url, 3, '美拍主播直播间信息详情页url：')
    print '美拍主播直播间信息详情页url：{}'.format(zhubo_live_url)
    response = requests.get(zhubo_live_url, headers=headers, timeout=10)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'lxml')
        owner_avatar = soup.select('img.avatar')
        if owner_avatar:
            owner_avatar = owner_avatar[0].attrs.get('src')
            if 'http' not in owner_avatar:
                owner_avatar = 'http:{}'.format(owner_avatar)
            print '头像url：', owner_avatar
        else:
            owner_avatar = ''

        online_str = r'<meta itemprop="interactionCount" content="(\d*)'
        search_obj = re.search(online_str, response.text, re.S)
        if search_obj:
            online_num = search_obj.group(1)
            print '在线人数：', online_num
            return {'online_num': online_num, 'owner_avatar': owner_avatar}
        else:
            print '美拍的在线人数解析失败'
            return None
    else:
        print '美拍主播的response_code is not 200'
        return None


def main():
    lock = Lock()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')    # 来源文件的时间
    date_p = time.strftime('%Y%m%d')    # 生成文件的时间

    path = "/ftp_samba/112/file_4spider/anchor_detail_url/"
    filename = path + 'anchor_detail_url_' + date + '_1.txt'  # 线上源文件  anchor_detail_url_20190218_1.txt
    # filename = r'C:\Users\xj.xu\Desktop\anchor_detail_url_20190218_1.txt'
    # filename = r'/home/spider/zhubo/anchor_detail_url_20190218_1.txt'
    if not os.path.exists(filename):
        file_name = os.listdir(path)[-1]
        filename = path + file_name
        print '目标文件不存在，获取该路径下的最新文件：', filename
    file = open(filename, 'r')
    for line in file:
        CHINAZ_DETAIL_URL_QUEUE.put(line)
    print '源数据队列中的数量：', CHINAZ_DETAIL_URL_QUEUE.qsize()

    dest_path = '/ftp_samba/112/spider/fanyule_two/zhubo/'  # 数据存储路径
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'zhubo_' + date_p)
    tmp_file_name = os.path.join(dest_path, 'zhubo_' + date_p + '.tmp')
    fileout = open(tmp_file_name, 'a')

    threads = []
    for t in xrange(50):
        t = threading.Thread(target=zhubo_detail, args=(fileout, lock))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        fileout.close()
    os.rename(tmp_file_name, dest_file_name)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'


if __name__ == '__main__':
    main()
    # zhubo_detail(['https://live.chinaz.com/zhubo/KK/131922490.html'])
    # douyu('1')
    # huya('1')
    # chushou('https://chushou.tv/room/86272226.htm?ptag=livechinaz')
    # bilibili('')
    # panda('http://www.panda.tv/902552?ptag=livechinaz')
    # huajiao()
    # CC('http://cc.163.com/555666/?ptag=livechinaz')
    # fangxin('http://fanxing.kugou.com/1512737?ptag=livechinaz')
    # longzhu('http://star.longzhu.com/m173002?ptag=livechinaz')
    # qf56('')
    # jiuxiu('')
    # renren('')
    # inke('')
    # kk('http://www.kktv5.com/show/20170601')
    # meipai('')


