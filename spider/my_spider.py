import json
import os
import re
import requests
import queue
import threading
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from threading import Lock

from .user_agent import MY_USER_AGENT
from .my_exception import FaildTooManyTimesException, CrawlCompletedException
from .utils import my_urljoin, get_func_name
from random import randint
from bs4 import BeautifulSoup
from progress.bar import ChargingBar


# TODO: 加上爬取间歇时间
class MySpider:
    """
    自己实现的用来构建自动化搜索引擎的爬取部分的爬虫类
    """

    def __init__(self, config_filepath, output_queue, ):
        self.__config_filepath__ = config_filepath
        self.__file_name_count__ = 0
        self.__file_name_count_lock__ = Lock()
        self.__config_json__ = self.__read_config_json_from_file__()
        self.__config__ = self.__read_config_from_file__()
        self.__not_access_queue__ = queue.PriorityQueue()
        self.__accessed_set__ = set()
        self.__cannot_access_set__ = set()
        self.__output_queue__ = output_queue
        self.__headers__ = http_header = {
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'Referer': 'http://www.baidu.com/',
        }
        self.__filter_urls_set__ = set()
        self.__url_compile_re__ = re.compile(r'^https?:/{2}\w.+$')
        self.__request_failed_nums__ = 2
        self.__allow_domain_regex_list__ = list()
        self.__progress_bar__ = ChargingBar('爬取网页', max=self.__config__['number_of_pages_to_crawl'])

    def __read_config_json_from_file__(self):
        try:
            with open(self.__config_filepath__, 'r') as f:
                config_json = json.loads(f.read())
                return config_json
        except FileNotFoundError:
            print('配置文件不存在，程序将结束')
            exit()

    def __getattr__(self, key):
        if key == 'config' or key == 'configs':
            return self.__config__

    # 配置读取器
    def __read_config_from_file__(self):
        config = dict()
        try:
            # 爬取开始的页面 Type:list
            config['begin_urls'] = self.__config_json__.get('BEGIN_URLS', None)
            if config['begin_urls'] == None:
                raise KeyError('配置文件填写不完整，begin_urls为必填配置')
            elif len(config['begin_urls']) == 0:
                raise KeyError('配置文件填写不完整，begin_urls不得为空')

            # 允许爬取的域名 Type:list Default:[]
            config['allow_domains'] = self.__config_json__.get('ALLOW_DOMAINS', [])

            # 需要过滤的域名 Type:list Default:[]
            config['filter_domains'] = self.__config_json__.get('FILTER_DOMAINS', [])

            # 需要优先爬取的域名 Type:list Default:[]
            config['priority_domains'] = self.__config_json__.get('PRIORITY_DOMIANS', [])

            # 需要爬取的页面数量 Type:int Default:None
            config['number_of_pages_to_crawl'] = self.__config_json__.get('NUMBER_OF_PAGES_TO_CRAWL', None)
            try:
                if config['number_of_pages_to_crawl'] is not None:
                    config['number_of_pages_to_crawl'] = int(config['number_of_pages_to_crawl'])
                if config['number_of_pages_to_crawl'] <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError('配置文件填写错误，NUMBER_OF_PAGES_TO_CRAWL必须为一个正数')

            # 爬取每个页面的超时时间 Type:int Default:None
            config['timeout_of_per_page'] = self.__config_json__.get('TIMEOUT_OF_PER_PAGE', None)
            try:
                if config['timeout_of_per_page'] is not None:
                    config['timeout_of_per_page'] = int(config['timeout_of_per_page'])
                if config['timeout_of_per_page'] <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError('配置文件填写错误，TIMEOUT_OF_PER_PAGE必须为一个正数')

            # 是否采用深度优先 Type:boolean Default:False
            config['is_depth_first'] = self.__config_json__.get('IS_DEPTH_FIRST', False)

            # 是否采用广度优先 Type:boolean Default:False
            config['is_breadth_first'] = self.__config_json__.get('IS_BREADTH_FIRST', False)

            if config['is_depth_first'] == True and config['is_breadth_first'] == True:
                raise ValueError('配置文件填写错误，IS_DEPTH_FIRST与IS_BREADTH_FIRST不能同时为True')
            if config['is_depth_first'] == False and config['is_breadth_first'] == False:
                config['is_depth_first'] = True

            # 线程池中线程数量 Type:int Default:机器CPU核心数*4
            config['threadpool_size'] = self.__config_json__.get('THREADPOOL_SIZE', cpu_count() * 4)
            try:
                config['threadpool_size'] = int(config['threadpool_size'])
                if config['threadpool_size'] <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError('配置文件填写错误，THREADPOOL_SIZE必须为一个正数')

            # 爬取下来的html文件的存储路径 Type:string default:当前工作目录下的html文件夹下
            config['html_file_storage_path'] = self.__config_json__.get('HTML_FILE_STORAGE_PATH',
                                                                        os.path.join(os.getcwd(), 'saved_html'))
            if not os.path.exists(config['html_file_storage_path']):
                os.mkdir(config['html_file_storage_path'])
            if not os.path.isdir(config['html_file_storage_path']):
                raise NotADirectoryError

            # TODO:数据库参数读取
            redis_confis = self.__config_json__.get('REDIS', None)
            if redis_confis:
                config['redis_host'] = redis_confis.get('HOST', 'localhost')
                config['redis_port'] = redis_confis.get('PORT', '6379')
                config['redis_db'] = redis_confis.get('db', 0)
                config['redis_pwd'] = redis_confis.get('PASSWORD', None)
            else:
                config['redis_host'] = 'localhost'
                config['redis_port'] = 6379
                config['redis_db'] = 0

            config['stop_words_path'] = self.__config_json__.get('STOP_WORDS_PATH', None)


            return config

        except (KeyError, ValueError, NotADirectoryError) as e:
            print(get_func_name(), ':', e)
            exit()

    # 读取初始urls
    def __read_begin_urls__(self):
        for i in self.__config__['begin_urls']:
            self.__not_access_queue__.put((0, i))

    # 读取需要过滤的urls
    def __read_need_filter_urls__(self):
        self.__filter_urls_set__.add('javascript:;')

    # 读取允许的urls
    def __read_allow_domain_urls__(self):
        for allow_domain in self.__config__['allow_domains']:
            self.__allow_domain_regex_list__.append(re.compile('.*' + allow_domain + '.*'))

    # 验证url是否满足allow_domians
    def __verify_url_for_allow_domains__(self, url):
        if(len(self.__allow_domain_regex_list__) == 0):
            return True
        for regx in self.__allow_domain_regex_list__:
            if (regx.match(url)):
                return True
        return False

    # 页面抓取器
    def __request__(self, url, priority, timeout=20, user_agent=MY_USER_AGENT[randint(0, len(MY_USER_AGENT) - 1)]):
        self.__headers__['User-Agent'] = user_agent
        # 请求预设的次数，若失败的次数达到预设的最大次数则将异常抛出
        # 若请求成功则跳出请求循环返回请求结果
        for i in range(self.__request_failed_nums__):
            try:
                req = requests.get(url, headers=self.__headers__, timeout=timeout)
                req.raise_for_status()
            except requests.ConnectionError:
                if i == self.__request_failed_nums__ - 1:
                    raise FaildTooManyTimesException()
            except requests.HTTPError:
                if req.status_code in [401, 403, 404, 500]:
                    raise FaildTooManyTimesException()
                else:
                    if i == self.__request_failed_nums__ - 1:
                        raise FaildTooManyTimesException()
            except requests.Timeout:
                if i == self.__request_failed_nums__ - 1:
                    raise FaildTooManyTimesException()
            else:
                break

        if self.__config__['is_breadth_first']:
            self.__breadth_first_getter__(priority, req.text, req.request.url)
        elif self.__config__['is_depth_first']:
            self.__depth_first_getter__(priority, req.text, req.request.url)
        return req.content

    @staticmethod
    def get_hostname(url):
        return url[:url.find('/', url.find('/', url.find('/') + 1) + 1) + 1]

    # url标准器
    def __url_standardizator__(self, url, hostname):
        if not self.__url_compile_re__.match(url):
            url = my_urljoin(hostname, url)
        return self.__url_cleaner__(url)

    # url分析器
    def __url_analyzer__(self, url):
        if url not in self.__accessed_set__ and \
                url not in self.__cannot_access_set__ and \
                self.__verify_url_for_allow_domains__(url):
            return True
        return False

    # url清洗器
    def __url_cleaner__(self, url):
        return url.rstrip('#')

    # 通用页面抓取器
    def __page__getter__(self, priority, page, url):
        doc = BeautifulSoup(page)
        url = MySpider.get_hostname(url)
        for i in doc.find_all('a'):
            try:
                new_url = i['href']

                # 过滤不需要的域名
                if new_url in self.__filter_urls_set__:
                    continue

                standard_url = self.__url_standardizator__(i['href'], url)
                if self.__url_analyzer__(standard_url):
                    self.__not_access_queue__.put((priority, standard_url))
            except KeyError:
                pass
            except Exception as e:
                # TODO:找出可能会发生的异常
                print(get_func_name(), ':', e)

    # 深度优先页面抓取器
    def __depth_first_getter__(self, priority, page, url):
        self.__page__getter__(priority + 1, page, url)

    # 广度优先页面抓取器
    def __breadth_first_getter__(self, priority, page, url):
        self.__page__getter__(priority - 1, page, url)

    # 线程创造器
    def __thread_creator__(self):
        return ThreadPoolExecutor(max_workers=self.__config__['threadpool_size'])

    def start(self):
        # TODO: 将优先队列中的url改为(优先级, url)的元组形式
        self.__read_begin_urls__()
        self.__read_need_filter_urls__()
        self.__read_allow_domain_urls__()
        executor = self.__thread_creator__()

        while True:
            try:
                url_tuple = self.__not_access_queue__.get()

                # 再次验证，以免重复爬取已爬取过的页面
                if url_tuple[1] in self.__accessed_set__:
                    continue

                future = executor.submit(self.__request__, url_tuple[1], url_tuple[0],
                                         self.__config__['timeout_of_per_page'])
                writed_page = future.result()

                # TODO:存储page
                self.__file_name_count_lock__.acquire()
                file_path = os.path.join(self.__config__['html_file_storage_path'],
                                         str(self.__file_name_count__) + '.html')
                self.__file_name_count__ += 1
                self.__file_name_count_lock__.release()

                with open(file_path, 'wb') as file:
                    file.write(url_tuple[1].encode() + b'\n')
                    file.write(writed_page)

                self.__output_queue__.put(file_path)
                self.__accessed_set__.add(url_tuple[1])
                # print(url_tuple[1], str(url_tuple[0]), 
                # str(self.__file_name_count__) + '/' + str(self.__config__['number_of_pages_to_crawl']))
                self.__progress_bar__.next()

                # 爬取完成判断
                if self.__file_name_count__ == self.__config__['number_of_pages_to_crawl']:
                    # raise CrawlCompletedException()
                    self.__output_queue__.put('mission_complete')
                    self.__progress_bar__.finish()
                    return
            # 若捕获到失败次数过多异常则将此请求的url放入不可访问链接集中
            # 并且取消本次请求
            except FaildTooManyTimesException:
                self.__cannot_access_set__.add(url_tuple[1])
                future.cancel()
