# coding:utf-8
__author__ = 'xxj'

import requests
import os
import time
import datetime
import Queue
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError
from socket import error
from rediscluster import StrictRedisCluster
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

ACCOUNT_PASSWORD_QUEUE = Queue.Queue()    # 账号、密码队列
PROXY_IP_Q = Queue.Queue()    # 代理ip队列
ACCOUNT_RETRY_COUNT = {}    # 账号、密码重试
baidu_account_password = [
    u'与城Zzc8l123nr/dijing80348',
    u'丶休止符cLaoi/dijing55695',
    u'loadingmyOfh/dijing13131',
    u'JumboCRAI20km/dijing42784',
    u'軟媃bTej/dijing17401',
    u'My丝袜年华2qvm/dijing95099',
    u'否寒Uu3re/dijing93369',
    u'富贵君君米1Igf/dijing63526',
    u'灵媒猫Wnxx/dijing80318',
    u'One_凡441o1ww/dijing22132',
    u'马哈加赞0K2hm/dijing73937',
    u'围宿6R4hi/dijing68726',
    u'妳de童话3rgc/dijing16371',
    u'home哈哈的l6yj/dijing55595',
    u'自嘲iz8Cmu/dijing52825',
    u'雷狼刃女0Lpq/dijing94859',
    u'你被困扰了Gcrf/dijing27472',
    u'HsuckyIGwd/dijing56445',
    u'残疾的云彩baac/dijing89878',
    u'Seret丶MvTks/dijing55725',
    u'出发n5vw/dijing56095',
    u'已被注册XFI3ya/dijing60326',
    u'天哥打碧莲Zmmx/dijing34263',
    u'D奶碍人717rz/dijing75027',
    u'雪天使木木5Csd/dijing02690',
    u'zc00100pB99si/dijing62246',
    u'one無罪8amo/dijing46234',
    u'我的丫丫呢ixhv/dijing60486',
    u'大手119Oind/dijing11681',
    u'大爱单3qru/dijing22712',
    u'啷嚟个啷hNye/dijing79487',
    u'嘻哈锡sdyz/dijing47944',
    u'丶瞧這貨DLsj/dijing10991',
    u'晋迁269av/dijing63156',
    u'ii碎念ii57my/dijing57305',
    u'人_生不易l6sx/jpu34701', 
    u'xdd877jG578mh/qqwwaass', 
    u'痴情菇凉_WNhc/qqwwaass', 
    u'q紫芊e9ko/qqwwaass', 
    u'公子逍逍gIso/qqwwaass', 
    u'白发凋零1Hhw/qqwwaass', 
    u'顾昕筱lvnv/kai48518', 
    u'a411855810I9de/zvy20756', 
    u'不良_某某_NWra/qqwwaass', 
    u'茶酥酥丷m2zk/cuv35015',
    '15542185364/gsviop', 
    '15542180354/gsviop', 
    '15542178614/gsviop', 
    '15988198594/hrx940903',
    '13867430385/sw@1234', 
    '18698575927/bd2712144', 
    u'嘻嘻哈哈927sky/123abc456', 
    'lyhhday/123abc456',
    'swtest24/sw@1234', 
    '13175066135/sw@1234', 
    u'双温莎结24qh/jvj72076', 
    u'pi子1g1uu/qqwwaass',
    '13463085977/sw@123456'
]


def get_redis_proxy():
    '''
    从redis相应的key中获取代理ip(读取快代理的代理ip)
    :return:
    '''
    startup_nodes = [{'host': 'redis2', 'port': '6379'}]
    r = StrictRedisCluster(startup_nodes=startup_nodes, decode_responses=True)
    baidu_zhishu_proxy_length = r.llen('spider:baidu_zhishu:proxy:kuai')  # baidu_zhishu
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中baidu_zhishu的代理ip长度：', baidu_zhishu_proxy_length
    if baidu_zhishu_proxy_length == 0:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'redis中的代理ip数量为0，等待60s'
        time.sleep(60)
        return get_redis_proxy()
    for i in xrange(20):
        ip = r.lpop('spider:baidu_zhishu:proxy:kuai')
        proxies = {
            'http': "http://{ip}".format(ip=ip),
            # 'https': "http://{ip}".format(ip=ip)
        }
        PROXY_IP_Q.put(proxies)


class BaiduZhishuException(Exception):
    def __init__(self, message):
        super(BaiduZhishuException, self).__init__()
        self.message = message


def baidu_login(baidu_username, baidu_password):
    '''
    百度指数登陆模块
    :return:
    '''
    global browser
    try:
        display = Display(visible=0, size=(1200, 600))
        display.start()
        time.sleep(3)

        # firefox_options = webdriver.FirefoxOptions()
        # firefox_options.set_headless()
        # firefox_options.add_argument('--disable-gpu')
        # profile = webdriver.FirefoxProfile()
        # # profile.set_preference('permissions.default.image', 2)  # 某些firefox只需要这个
        # browser = webdriver.Firefox(firefox_profile=profile, options=firefox_options)

        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('no-sandbox')
        browser = webdriver.Chrome(chrome_options=chrome_options)

        print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'chrome配置加载完成'
        wait = WebDriverWait(browser, 30)
        browser.get('http://index.baidu.com/?from=pinzhuan')
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '进入百度指数主页面'
        login_page = wait.until(
            EC.presence_of_element_located((By.XPATH, '//span[@class="username-text"]'))
        )    # 登录入口
        login_page.click()
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '进入登录页面'
        time.sleep(5)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), browser.current_url
        cookie_list = browser.get_cookies()
        cookie_dict = {}
        for cookie in cookie_list:
            if cookie.has_key('name') and cookie.has_key('value'):
                cookie_dict[cookie['name']] = cookie['value']
        cookie = 'BDUSS={BDUSS}'.format(BDUSS=cookie_dict.get('BDUSS'))
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '登录前的cookie：', cookie

        # pic_path = r'/ftp_samba/112/spider/fanyule_two/baidu_zhishu/login_before.png'
        # browser.save_screenshot(pic_path)

        username = wait.until(
            EC.presence_of_element_located((By.XPATH, '//input[@id="TANGRAM__PSP_4__userName"]'))
        )    # 用户名
        username.send_keys(baidu_username)
        time.sleep(2)
        password = wait.until(
            EC.presence_of_element_located((By.XPATH, '//input[@id="TANGRAM__PSP_4__password"]'))
        )    # 密码
        password.send_keys(baidu_password)

        # pic_path = r'/ftp_samba/112/spider/fanyule_two/baidu_zhishu/input.png'
        # browser.save_screenshot(pic_path)

        time.sleep(2)
        login = wait.until(
            EC.presence_of_element_located((By.XPATH, '//input[@id="TANGRAM__PSP_4__submit"]'))
        )    # 登录按钮
        login.send_keys(Keys.ENTER)
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '百度指数已登录'
        time.sleep(10)
        cookie_list = browser.get_cookies()
        cookie_dict = {}
        for cookie in cookie_list:
            if cookie.has_key('name') and cookie.has_key('value'):
                cookie_dict[cookie['name']] = cookie['value']
        cookie = 'BDUSS={BDUSS}'.format(BDUSS=cookie_dict.get('BDUSS'))
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '登录后的cookie：', cookie

        # pic_path = r'/ftp_samba/112/spider/fanyule_two/baidu_zhishu/login_after.png'
        # browser.save_screenshot(pic_path)

        browser.quit()
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '关闭游览器'
        return cookie

    except error as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '百度指数登录异常error', e
        time.sleep(5)

    except BaseException as e:
        print time.strftime('[%Y-%m-%d %H:%M:%S]'), '百度指数登录异常BaseException', e
        browser.quit()
        time.sleep(5)
        return baidu_login(baidu_username, baidu_password)


def baidu_zhishu(fileout, keyword_ls, proxies):
    '''
    百度指数
    :return:
    '''
    account_password = ACCOUNT_PASSWORD_QUEUE.get()    # 获取账号、密码
    if ACCOUNT_RETRY_COUNT.get(account_password) is None:
        ACCOUNT_RETRY_COUNT[account_password] = 1
        ACCOUNT_PASSWORD_QUEUE.put(account_password)
        print '账号、密码重试MAP：', ACCOUNT_RETRY_COUNT
    username = account_password.split('/')[0]
    password = account_password.split('/')[1]
    print 'username:', username
    print 'password:', password
    cookie = baidu_login(username, password)    # 百度登录接口
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
        'Cookie': cookie
        # 'Cookie': 'BDUSS=jZuN3djOUt3SGVpbXBtQ2dvRFZJUkxlSnl5OGY2Ti1-R0pmUm9qdWFwOWZUU3hjQVFBQUFBJCQAAAAAAAAAAAEAAAD1Txlc0KHLp73cw8gAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAF~ABFxfwARcR'
    }
    for line in keyword_ls:
        line_ls = line.split('\t')
        keyword = line_ls[0]    # 关键词
        try:
            search_type, search_avg = baidu_search_zhishu(headers, keyword, proxies)    # 百度搜索指数接口
            # 字段：关键词、指数类型、指数值
            if search_avg == '关键词未被收录':
                search_content = '\t'.join([line, '搜索指数', ''])
                fileout.write(search_content)
                fileout.write('\n')
                info_content = '\t'.join([line, '资讯指数', ''])
                fileout.write(info_content)
                fileout.write('\n')
                # media_content = '\t'.join([line, '媒体指数', ''])
                # fileout.write(media_content)
                # fileout.write('\n')
                fileout.flush()
            elif search_avg == 0:
                search_content = '\t'.join([line, '搜索指数', '0'])
                fileout.write(search_content)
                fileout.write('\n')
                info_content = '\t'.join([line, '资讯指数', '0'])
                fileout.write(info_content)
                fileout.write('\n')
                # media_content = '\t'.join([line, '媒体指数', '0'])
                # fileout.write(media_content)
                # fileout.write('\n')
                fileout.flush()
            else:
                info_type, info_avg = baidu_information_zhishu(headers, keyword, proxies)    # 百度资讯指数接口
                # media_type, media_avg = baidu_media_zhishu(headers, keyword, proxies)    # 百度媒体指数接口
                search_content = '\t'.join([line, search_type, str(search_avg)])
                fileout.write(search_content)
                fileout.write('\n')
                info_content = '\t'.join([line, info_type, str(info_avg)])
                fileout.write(info_content)
                fileout.write('\n')
                # media_content = '\t'.join([line, media_type, str(media_avg)])
                # fileout.write(media_content)
                # fileout.write('\n')
                fileout.flush()
        except BaiduZhishuException as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), '状态码异常', e.message, line
            if e.message == 'retry login':
                # 重新登录新的账号、密码
                # browser.close()
                if ACCOUNT_PASSWORD_QUEUE.qsize() == 0:
                    return False    # 自动退出项目
                account_password = ACCOUNT_PASSWORD_QUEUE.get()
                if ACCOUNT_RETRY_COUNT.get(account_password) is None:
                    ACCOUNT_RETRY_COUNT[account_password] = 1
                    ACCOUNT_PASSWORD_QUEUE.put(account_password)
                    print '账号、密码重试MAP：', ACCOUNT_RETRY_COUNT
                elif ACCOUNT_RETRY_COUNT.get(account_password) < 3:
                    ACCOUNT_RETRY_COUNT[account_password] = ACCOUNT_RETRY_COUNT.get(account_password) + 1
                    ACCOUNT_PASSWORD_QUEUE.put(account_password)
                    print '账号、密码重试MAP：', ACCOUNT_RETRY_COUNT
                else:
                    print '账号、密码重试MAP：', ACCOUNT_RETRY_COUNT
                username = account_password.split('/')[0]
                password = account_password.split('/')[1]
                print 'username:', username
                print 'password:', password
                cookie = baidu_login(username, password)
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
                    'Cookie': cookie
                }
                keyword_ls.append(line)

        except ReadTimeout as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ReadTimeout异常', line
            keyword_ls.append(line)

        except ConnectTimeout as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ConnectTimeout异常', line
            keyword_ls.append(line)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            print '新的代理IP：', proxies

        except ConnectionError as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'ConnectionError异常', line
            keyword_ls.append(line)
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            print '新的代理IP：', proxies

        except BaseException as e:
            print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'BaseException异常', line, e
            if PROXY_IP_Q.empty():
                get_redis_proxy()
                print '获取到新代理队列中代理ip数量：', PROXY_IP_Q.qsize()
            proxies = PROXY_IP_Q.get(False)
            print '新的代理IP：', proxies
            if e.message == 'No JSON object could be decoded':
                print 'get No JSON object could be decoded'
                keyword_ls.append(line)
            else:
                print 'e:', e, 'e.message:', e.message
            

def baidu_search_zhishu(headers, keyword, proxies):
    '''
    百度搜索指数
    :param headers:
    :param keyword:
    :return:
    '''
    url = 'http://index.baidu.com/api/SearchApi/index?area=0&word={word}&days=1'.format(word=keyword)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '百度搜索指数url：', url, headers.get('Cookie'), proxies
    response = requests.get(url=url, headers=headers, proxies=proxies, timeout=1)
    # print response.text  # {"status":10000,"data":"","message":"not login"} 未登录状态
    response_json = response.json()
    status = response_json.get('status')    # 状态码
    # print '状态吗类型：',  type(status)
    if status == 10000:    # 百度指数未登录
        raise BaiduZhishuException('retry login')
    elif status == 10001:    # 访问过于频繁，被限制
        raise BaiduZhishuException('retry login')
    elif status == 10002:    # 该关键词未被收录
        return '搜索指数', '关键词未被收录'
    avg = response_json.get('data').get('generalRatio')[0].get('all').get('avg')
    if avg is not None:
        print '百度搜索日均值：', avg
    elif avg == 0:
        return '搜索指数', 0
    else:
        print '搜索指数响应内容需分析：', response_json
        avg = ''
    return '搜索指数', avg


def baidu_information_zhishu(headers, keyword, proxies):
    '''
    百度资讯指数
    :param headers:
    :param keyword:
    :return:
    '''
    url = 'http://index.baidu.com/api/FeedSearchApi/getFeedIndex?area=0&word={word}&days=1'.format(word=keyword)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '百度资讯指数：', url, headers.get('Cookie'), proxies
    response = requests.get(url=url, headers=headers, proxies=proxies, timeout=1)
    # print response.text  # {"status":10000,"data":"","message":"not login"} 未登录状态
    response_json = response.json()
    status = response_json.get('status')  # 状态码
    # print '状态吗类型：', status
    if status == 10000:  # 百度指数未登录
        raise BaiduZhishuException('retry login')
    elif status == 10001:  # 访问过于频繁，被限制
        raise BaiduZhishuException('retry login')
    elif status == 10002:  # 该关键词未被收录
        return '资讯指数', '关键词未被收录'
    avg = response_json.get('data').get('index')[0].get('generalRatio').get('avg')  # 整体日平均值
    if avg is not None:
        print '百度资讯日均值：', avg
    else:
        print '资讯指数响应内容需分析：', response_json
        avg = ''
    return '资讯指数', avg


# def baidu_media_zhishu(headers, keyword, proxies):
#     '''
#     百度媒体指数
#     :param headers:
#     :param keyword:
#     :return:
#     '''
#     url = 'http://index.baidu.com/api/NewsApi/getNewsIndex?area=0&word={word}&days=1'.format(word=keyword)
#     print time.strftime('[%Y-%m-%d %H:%M:%S]'), '百度媒体指数：', url, keyword, proxies
#     response = requests.get(url=url, headers=headers, proxies=proxies, timeout=1)
#     # print response.text  # {"status":10000,"data":"","message":"not login"} 未登录状态
#     response_json = response.json()
#     status = response_json.get('status')  # 状态码
#     # print '状态吗类型：', status
#     if status == 10000:  # 百度指数未登录
#         raise BaiduZhishuException('retry login')
#     elif status == 10001:  # 访问过于频繁，被限制
#         raise BaiduZhishuException('retry login')
#     elif status == 10002:  # 该关键词未被收录
#         return '媒体指数', '关键词未被收录'
#     avg = response_json.get('data').get('index')[0].get('generalRatio').get('avg')  # 整体日平均值
#     if avg is not None:
#         print '百度媒体日均值：', avg
#     else:
#         print '媒体指数响应内容需分析：', response_json
#         avg = ''
#     return '媒体指数', avg


def main():
    yesterday = datetime.date.today() + datetime.timedelta(-1)
    date = yesterday.strftime('%Y%m%d')
    file_time = time.strftime('%Y%m%d')
    keyword_file_dir = r'/ftp_samba/112/file_4spider/bdzs_keyword/'  # 游戏的来源目录
    keyword_file_name = r'bdzs_keyword_{date}_1.txt'.format(date=date)  # 游戏的来源文件名
    keyword_file_path = os.path.join(keyword_file_dir, keyword_file_name)
    # keyword_file_path = r'C:\Users\xj.xu\Desktop\dmn_fanyule2_game_20181202_1.txt'
    print '获取来源文件：', keyword_file_path
    while True:
        if os.path.exists(keyword_file_path):
            break
        time.sleep(60)
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '游戏文件路径：', keyword_file_path
    keyword_file = open(keyword_file_path, 'r')
    keyword_ls = []
    for line in keyword_file:
        line = line.strip()
        if line:
            keyword_ls.append(line)
    print '数据来源关键词的数量：', len(keyword_ls)

    for account_password in baidu_account_password:
        ACCOUNT_PASSWORD_QUEUE.put(account_password)
    print '百度账号密码队列中的数量：', ACCOUNT_PASSWORD_QUEUE.qsize()

    get_redis_proxy()  # 将redis中的代理ip放入到PROXY_IP_Q队列中
    proxy_count = PROXY_IP_Q.qsize()  # 根据代理队列中代理的数量来决定线程数
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), '代理ip队列中的ip数量：', proxy_count
    proxies = PROXY_IP_Q.get(False)
    print '代理ip：', proxies

    dest_path = '/ftp_samba/112/spider/fanyule_two/baidu_zhishu/'  # linux上的文件目录
    # dest_path = os.getcwd()    # windows上的文件目录
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    dest_file_name = os.path.join(dest_path, 'baidu_zhishu_' + file_time)
    tmp_file_name = os.path.join(dest_path, 'baidu_zhishu_' + file_time + '.tmp')
    fileout = open(tmp_file_name, 'w')
    baidu_zhishu(fileout, keyword_ls, proxies)
    fileout.flush()
    fileout.close()
    os.rename(tmp_file_name, dest_file_name)
    # browser.close()


if __name__ == '__main__':
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'start'
    main()
    print time.strftime('[%Y-%m-%d %H:%M:%S]'), 'end'
