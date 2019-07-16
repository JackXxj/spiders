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
import math
import selenium
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.support.wait import WebDriverWait
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import json

reload(sys)
sys.setdefaultencoding('utf-8')
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36',
    }
PROXY_IP_Q = Queue.Queue()     # 代理ip队列
url_count = 0    # 统计项目请求数


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
            PROXY_IP_Q.put(ip)


def get(url, count, url_des):
    global ip
    for i in xrange(count):
        response = r(url, i, url_des)
        if response is None:    # 异常
            pass
        elif response.status_code == 200:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '状态码：', 200
            response_obj = lxml.etree.HTML(response.text)
            bread_c = response_obj.xpath('//div[@class="bread J_bread"]/span/text()')
            if bread_c:
                return response
            else:
                print 'cookieid失效，重新获取cookieid'
                get_cookieid()  # 重新获取cookieid    (目前知道这两种需要重新获取cookieid)
        elif response.status_code == 403:    # 重新获取代理ip和cookieid
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '状态码：', 403
            get_cookieid()  # 重新获取cookieid
        else:
            print '其他响应状态码：', response.status_code
        if PROXY_IP_Q.empty():
            get_redis_proxy()
            ip_num = PROXY_IP_Q.qsize()
            print '代理ip为空，重新获取到的代理ip数量：', ip_num
        ip = PROXY_IP_Q.get(False)
        print '获取到新的代理ip：', ip

    return None


def r(url, i, url_des):
    global url_count
    try:
        proxies = {
            'http': "http://{ip}".format(ip=ip),
            'https': "http://{ip}".format(ip=ip)
        }
        headers.update({'Cookie': '_hc.v={cookieid}'.format(cookieid=cookieid)})

        print time.strftime('[%Y-%m-%d %H:%M:%S]'), url_des, url, '代理proxies：', proxies, 'headers：', headers, 'count：', i
        url_count += 1
        response = requests.get(url=url, headers=headers, proxies=proxies, timeout=10)
    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException：', e.message, 'url：', url
        response = None
        # time.sleep(1)
    return response


def get_city():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.84 Safari/537.36',
    }
    citys = ['北京', '上海', '广州', '深圳', '成都', '杭州', '武汉', '重庆', '南京', '天津', '苏州', '西安', '长沙', '沈阳', '青岛', '郑州', '大连', '东莞', '宁波', '厦门', '福州', '无锡', '合肥', '昆明', '哈尔滨', '济南', '佛山', '长春', '温州', '石家庄', '南宁', '常州', '泉州', '南昌', '贵阳', '太原', '烟台', '嘉兴', '南通', '金华', '珠海', '惠州', '徐州', '海口', '乌鲁木齐', '绍兴', '中山', '台州', '兰州']
    url = 'https://m.dianping.com/citylist'
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '城市列表页：', url
    response = requests.get(url=url, headers=headers, timeout=5)
    # response = get(url, 3, headers)
    response_obj = lxml.etree.HTML(response.text)
    char_list = response_obj.xpath('//div[@class="home-place-list letter-list"]/ul/li/a/text()')  # 字母列表（具有以该字母为首字母的城市）
    # print '具有城市的首字母：', len(char_list)
    city_dict_cy = {}  # 城市--cy值 字典
    for char in char_list:
        url = 'https://m.dianping.com/citylist?c={char}&returl=&type='.format(char=char)  # 每个首字母城市的url
        print time.strftime('[%Y-%m-%d %H:%M:%S] '), '城市详情页：', url
        response = requests.get(url=url, headers=headers, timeout=5)
        # response = get(url, 3, headers)
        response_obj = lxml.etree.HTML(response.text)
        city_list = response_obj.xpath('//ul[@class="J_citylist"]/li/a')  # 首字母下的城市
        for city in city_list:
            city_name = city.xpath('./text()')[0]  # 城市名称
            cy = city.xpath('./@data-id')[0]  # 城市cy
            for city_x in citys:
                if city_x == city_name:
                    # print '城市：', city_x
                    # print '大众点评的城市：', city_name
                    # print 'cy：', cy
                    city_dict_cy[city_x] = cy
    print '所有城市的数量：', len(city_dict_cy)
    print city_dict_cy
    return city_dict_cy


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


def get_cookieid():
    global cookieid, ip, url_count
    try:
        display = Display(visible=0, size=(1000, 800))
        display.start()
        time.sleep(1)

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('no-sandbox')
        chrome_options.add_argument('--disable-gpu')
        proxy_https_argument = '--proxy-server=http://{ip}'.format(ip=ip)  # http, https (无密码，或白名单ip授权，成功)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'selenium中的代理配置：', proxy_https_argument
        chrome_options.add_argument(proxy_https_argument)
        driver = webdriver.Chrome(chrome_options=chrome_options)
        wait = WebDriverWait(driver, 10)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'selenium请求https://www.dianping.com/'
        url_count += 1
        driver.get('https://www.dianping.com/')
        wait.until(
            EC.presence_of_element_located((By.XPATH, '//input[@id="J-search-input"]')), message='大众点评搜索框未找到'
        )
        # time.sleep(10)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'selenium请求https://www.dianping.com/search/keyword/3/0_%E7%BD%91%E9%B1%BC%E7%BD%91%E5%92%96'
        url_count += 1
        driver.get('https://www.dianping.com/search/keyword/3/0_%E7%BD%91%E9%B1%BC%E7%BD%91%E5%92%96')
        wait.until(
            EC.presence_of_element_located((By.XPATH, '//input[@id="J-search-input"]')), message='大众点评搜索框未找到'
        )
        # time.sleep(10)
        content = driver.page_source
        # print '内容:', content
        search_obj = re.search(r'cookieid: \'(.*?)\',', content, re.S)
        if search_obj:
            cookieid = search_obj.group(1)
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'cookieid：', cookieid
            # return cookieid
        else:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '获取cookieid的正则表达式失效，重新分析'
            if display:
                display.stop()
            if driver:
                driver.quit()
            time.sleep(5)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                ip_num = PROXY_IP_Q.qsize()
                print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip为空，重新获取到的代理ip数量：', ip_num
            ip = PROXY_IP_Q.get(False)
            return get_cookieid()
    except WebDriverException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'WebDriverException异常：', e
        if display:
            display.stop()
        if driver:
            driver.quit()
        if PROXY_IP_Q.empty():
            get_redis_proxy()
            ip_num = PROXY_IP_Q.qsize()
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip为空，重新获取到的代理ip数量：', ip_num
        ip = PROXY_IP_Q.get(False)
        return get_cookieid()
    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException异常：', e
        if display:
            display.stop()
        if driver:
            driver.quit()
        if PROXY_IP_Q.empty():
            get_redis_proxy()
            ip_num = PROXY_IP_Q.qsize()
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip为空，重新获取到的代理ip数量：', ip_num
        ip = PROXY_IP_Q.get(False)
        return get_cookieid()

    else:
        if display:
            display.stop()
        if driver:
            driver.quit()


def da_zhong_dian_ping(fileout):
    url_demo = 'http://www.dianping.com/search/keyword/{city_id}/0_{net_name}/p{page}'
    net_names = get_net_name()  # 获取网吧名称列表接口
    city_id_dict = get_city()  # 获取city--id关系接口
    for city_name, city_id in city_id_dict.items():
        print '城市名称：', city_name
        print '城市id：', city_id
        for net_name in net_names:
            net_name = net_name.decode('gbk')    # 根据实际线上需调整
            print '网吧名称：', net_name
            url = url_demo.format(city_id=city_id, net_name=net_name, page=1)    # 第一页
            # response = requests.get(url, headers=headers, timeout=10)
            # print '状态码：', response.status_code
            response = get(url, 5, '大众点评网第一页url：')
            if response is not None:
                response_obj = lxml.etree.HTML(response.text)
                bread_c = response_obj.xpath('//div[@class="bread J_bread"]/span/text()')    # 获取某地区的网吧的网点数量（虚的）
                if bread_c:
                    bread_c = bread_c[0]
                    # print bread_c
                    search_obj = re.search(r'\d+', bread_c, re.S)
                    if search_obj:
                        total = int(search_obj.group())
                        print '网点总量：', total
                    else:
                        print '获取总数量的正则表达式解析失败, 重新分析'
                    # total = span_ls
                    if total != 0:
                        net_count = 0    # 统计有效的网点
                        li_objs = response_obj.xpath('//div[@id="shop-all-list"]/ul/li')
                        # print '长度：', len(li_objs)
                        for li_obj in li_objs:
                            detail_url = li_obj.xpath('./div[@class="txt"]/div[@class="tit"]/a/@href')[0]    # url
                            # print '详情页url：', detail_url
                            title = li_obj.xpath('./div[@class="txt"]/div[@class="tit"]/a/h4/text()')[0]    # 名称
                            # print '名称：', title
                            address = li_obj.xpath('./div[@class="operate J_operate Hide"]/a[@class="o-map J_o-map"]/@data-address')[0]    # 地址
                            # print '地址：', address
                            content = {'city_name': city_name, 'name': net_name, 'url': detail_url, 'title': title,
                                       'address': address}  # 网点信息
                            content = json.dumps(content, ensure_ascii=False)
                            fileout.write(content)
                            fileout.write('\n')
                            fileout.flush()
                            if net_name in title:    # 筛选出有效的网点信息
                                net_count += 1
                        if net_count >= 8:    # 根据每一页的有效网点的统计，判断下一页中是否具有满足条件的网点。
                        # if net_name in title:    # 根据每一页的最后一条(网点)判断下一页中是否具有满足条件的网点(该策略的效果不佳)
                            all_page = int(math.ceil(total/15.0))
                            print '网点总页数：', all_page
                            if all_page >= 2:
                                for page in xrange(2, all_page+1):
                                    net_count = 0
                                    url = url_demo.format(city_id=city_id, net_name=net_name, page=page)
                                    # response = requests.get(url, headers=headers, timeout=10)
                                    # print '状态码：', response.status_code
                                    response = get(url, 5, '大众点评网索引页url：')
                                    if response is not None:
                                        response_obj = lxml.etree.HTML(response.text)
                                        li_objs = response_obj.xpath('//div[@id="shop-all-list"]/ul/li')
                                        print '长度：', len(li_objs)
                                        for li_obj in li_objs:
                                            detail_url = li_obj.xpath('./div[@class="txt"]/div[@class="tit"]/a/@href')[0]  # url
                                            # print '详情页url：', detail_url
                                            title = li_obj.xpath('./div[@class="txt"]/div[@class="tit"]/a/h4/text()')[0]  # url
                                            # print '名称：', title
                                            address = li_obj.xpath('./div[@class="operate J_operate Hide"]/a[@class="o-map J_o-map"]/@data-address')[0]  # 地址
                                            # print '地址：', address
                                            content = {'city_name': city_name, 'name': net_name, 'url': detail_url,
                                                       'title': title, 'address': address}  # 网点信息
                                            content = json.dumps(content, ensure_ascii=False)
                                            fileout.write(content)
                                            fileout.write('\n')
                                            fileout.flush()
                                            if net_name in title:  # 筛选出有效的网点信息
                                                net_count += 1

                                        if net_count < 8:  # 对每一页的网点评估，大于等于8个就进行下一页网点的获取
                                            print '该网点下一页无效， net_count：', net_count
                                            break

                                    else:
                                        print '大众点评网索引页的response is None'
                        else:
                            print '该网点只有一页有效，net_count：', net_count

                    else:
                        print '该城市<{city}>的网吧<{net_name}>的数量为0, url:{url}'.format(city=city_name, net_name=net_name, url=url)
                else:
                    print '无法获取到总页数'
                    print response.text
            else:
                print '大众点评网第一页的response is None'


def main():
    date = time.strftime('%Y%m%d')
    dest_path = '/ftp_samba/112/spider/python/internet_bar/dazhongdianping/'  # 数据存储目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'dazhongdianping_netbar_' + date)
    tmp_file_name = os.path.join(dest_path, 'dazhongdianping_netbar_' + date + '.tmp')
    fileout = open(tmp_file_name, 'w')
    get_cookieid()    # 获取cookieid接口
    da_zhong_dian_ping(fileout)    # 大众点评网接口

    print '大众点评项目的总url请求数：', url_count
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
    ip = PROXY_IP_Q.get()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ip：', ip
    main()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'



