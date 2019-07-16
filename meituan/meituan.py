# coding:utf-8 
__author__ = 'xxj'

import time
import requests
import sys
import lxml.etree
import redis
import Queue
import re
import datetime
import os
import selenium
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support import expected_conditions as EC
import json

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36',
    }
PROXY_IP_Q = Queue.Queue()     # 代理ip队列


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip
    :return:
    '''
    r = redis.StrictRedis(host="172.31.10.75", port=9221)
    kuai_proxy_length = r.scard('spider:kuai:proxy')  # 快代理
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中kuai的代理ip长度：', kuai_proxy_length
    if kuai_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    kuai_proxy_set = r.smembers('spider:kuai:proxy')    # 快代理集合
    for i, ip in enumerate(kuai_proxy_set):
        if i == 20:
            break
        else:
            proxies = {
                'http': "http://{ip}".format(ip=ip),
                'https': "http://{ip}".format(ip=ip)
            }
            PROXY_IP_Q.put(proxies)


def get(url_demo, count, url_des):
    global proxies
    for i in xrange(count):
        url = url_demo.format(uuid=uuid)
        response = r(url, i, url_des)
        if response is None:    # 异常
            pass
        elif response.status_code == 200:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '状态码：', 200
            return response
        elif response.status_code == 403:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '状态码：', 403, '重新获取有效的uuid'
            get_uuid()     # uuid接口
        else:
            print '其他响应状态码：', response.status_code
        if PROXY_IP_Q.empty():
            get_redis_proxy()
            ip_num = PROXY_IP_Q.qsize()
            print '代理ip为空，重新获取到的代理ip数量：', ip_num
        proxies = PROXY_IP_Q.get(False)
    return None


def r(url, i, url_des):
    try:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), url_des, url, '代理proxies：', proxies, 'count：', i
        response = requests.get(url=url, headers=headers, proxies=proxies, timeout=10)
    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException：', e.message, 'url：', url
        response = None
        # time.sleep(1)
    return response


def get_uuid():
    global uuid
    try:
        display = Display(visible=0, size=(1000, 800))
        display.start()
        time.sleep(1)

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        # proxy_https_argument = '--proxy-server=http://{ip}'.format(ip=ip)  # http, https (无密码，或白名单ip授权，成功)
        # print 'proxy_https_argument：', proxy_https_argument
        # chrome_options.add_argument(proxy_https_argument)
        driver = webdriver.Chrome(chrome_options=chrome_options)
        wait = WebDriverWait(driver, 10)
        driver.get('https://bj.meituan.com/')
        time.sleep(10)
        driver.get('https://sh.meituan.com/s/%E7%BD%91%E9%B1%BC%E7%BD%91%E5%92%96/')
        time.sleep(10)
        content = driver.page_source
        # print '内容:', content
        search_obj = re.search(r'<script>window.AppData = (.*?);</script>', content, re.S)
        if search_obj:
            uuid_info = search_obj.group(1)
            uuid_dict = json.loads(uuid_info)
            uuid = uuid_dict.get("poiParam").get('uuid')
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'uuid：', uuid
            return uuid
        else:
            print '获取uuid的正则表达式失效，重新分析'
    except WebDriverException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'WebDriverException异常：', e
        if display:
            display.stop()
        if driver:
            driver.quit()
        return get_uuid()
    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException异常：', e
    finally:
        if display:
            display.stop()
        if driver:
            driver.quit()


def get_city():
    '''
    获取抓取的city与city_id关系
    :return:
    '''
    city_id_dict = {}
    url = 'https://www.meituan.com/changecity/'
    citys = ['北京', '上海', '广州', '深圳', '成都', '杭州', '武汉', '重庆', '南京', '天津', '苏州', '西安', '长沙', '沈阳', '青岛', '郑州', '大连', '东莞', '宁波', '厦门', '福州', '无锡', '合肥', '昆明', '哈尔滨', '济南', '佛山', '长春', '温州', '石家庄', '南宁', '常州', '泉州', '南昌', '贵阳', '太原', '烟台', '嘉兴', '南通', '金华', '珠海', '惠州', '徐州', '海口', '乌鲁木齐', '绍兴', '中山', '台州', '兰州']
    print '匹配前的city数量：', len(citys)
    # response = requests.get(url, headers=headers, timeout=10)
    response = get(url, 10, '获取city与city_id关系的url：')
    if response is not None:
        response_text = response.text
        search_obj = re.search(r'<script>window.AppData = (.*?);</script>', response_text, re.S)
        if search_obj:
            city_info = search_obj.group(1)
            all_city_dict = json.loads(city_info)
            # print city_id_dict
            all_city_list = all_city_dict.get('openCityList')
            # print all_city_list, len(all_city_list)
            for alpha_city_list in all_city_list:    # 遍历字母系列城市列表
                city_list = alpha_city_list[1]    # 字母系列城市列表
                # print city_list
                for city in city_list:    # 遍历某项字母列表中的每个城市信息字典
                    city_name = city.get('name')    # 城市名称
                    for need_city_name in citys:
                        if city_name == need_city_name:
                            # print '城市名称：', city_name
                            id = city.get('id')    # 城市ID
                            # print 'city_id：', id
                            city_id_dict[city_name] = id
            print '匹配后的city数量：', len(citys)
            return city_id_dict

        else:
            print '正则表达式解析失败，重新解析...'
    else:
        print '获取city与city_id关系的response is None'


def get_net_name():
    '''
    需要抓取的网吧
    :return:
    '''
    net_names = []
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')
    keyword_file_dir = r'/ftp_samba/112/spider/python/ip/apnic'  # 来源目录
    keyword_file_name = r'apnic_{date}.txt'.format(date=date)  # 来源文件名
    keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
    keyword_file_path = r'/root/spider/python/test/net_bar/meituan/data_20196251431.txt'
    print '获取来源文件：', keyword_file_path
    if not os.path.exists(keyword_file_path):
        print '不存在该文件，获取该目录下最新文件'
        keyword_file_name = os.listdir(keyword_file_dir)[-1]
        keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取到最新的文件路径：', keyword_file_path
    keyword_file = open(keyword_file_path, 'r')

    for line in keyword_file:
        line = line.strip()
        if line:
            net_names.append(line)
    print '网吧的数量：', len(net_names)
    return net_names


def meituan(fileout):
    url_demo = 'https://apimobile.meituan.com/group/v4/poi/pcsearch/{city_id}?uuid={uuid}&userid=-1&limit={limit}&offset=0&cateId=-1&q={name}'
    net_names = get_net_name()    # 获取网吧名称列表接口
    city_id_dict = get_city()    # 获取city--id关系接口
    for city_name, city_id in city_id_dict.items():
        print '城市名称：', city_name
        print '城市id：', city_id
        # nets = []    # 一个城市下所有网吧连锁列表
        for net_name in net_names:
            # net_bars = []    # 网吧连锁的网点信息列表
            net_name = net_name.decode('gbk')    # 根据实际线上需调整
            print '网吧名称：', net_name
            url = url_demo.format(city_id=city_id, uuid='{uuid}', limit=1000, name=net_name)
            # print 'url：', url
            # response = requests.get(url, headers=headers, timeout=10)
            # print '状态码：', response.status_code
            response = get(url, 5, '美团索引页url：')
            if response is not None:
                response_json = response.json()
                code = response_json.get('code')
                if code == '0':
                    data = response_json.get('data')
                    total_count = data.get('totalCount')    # 网点总量
                    print '该城市<{city}>的网吧<{net_name}>的总量: {total}'.format(city=city_name, net_name=net_name, total=total_count)
                    search_results = data.get('searchResult')
                    if search_results:
                        for search_result in search_results:
                            id = search_result.get('id')    # id
                            detail_url = 'https://www.meituan.com/xiuxianyule/{id}/'.format(id=id)    # 详情页url
                            name = search_result.get('title')  # 名称
                            # print '名称：', name
                            address = search_result.get('address')    # 地址
                            # print '地址：', address
                            content = {'city_name': city_name, 'name': net_name, 'url': detail_url, 'title': name, 'address': address}    # 网点信息
                            content = json.dumps(content, ensure_ascii=False)
                            fileout.write(content)
                            fileout.write('\n')
                            fileout.flush()
                    else:
                        print '该城市<{city}>的网吧<{net_name}>的数量为0, url:{url}'.format(city=city_name, net_name=net_name, url=url.format(uuid=uuid))

                else:
                    print 'response_json中的code非0：', code

            else:
                print '美团索引页的response is None'


def main():
    date = time.strftime('%Y%m%d')
    dest_path = '/ftp_samba/112/spider/python/internet_bar/meituan/'  # 数据存储目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'meituan_netbar_' + date)
    tmp_file_name = os.path.join(dest_path, 'meituan_netbar_' + date + '.tmp')
    fileout = open(tmp_file_name, 'w')
    get_uuid()    # 获取uuid接口
    meituan(fileout)    # 美团接口
    try:
        fileout.flush()
        fileout.close()
    except IOError as e:
        time.sleep(1)
        fileout.close()

    os.rename(tmp_file_name, dest_file_name)


if __name__ == '__main__':
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    get_redis_proxy()
    ip_num = PROXY_IP_Q.qsize()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取到的代理ip数量：', ip_num
    proxies = PROXY_IP_Q.get()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'proxies：', proxies
    main()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'





