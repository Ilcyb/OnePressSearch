# coding: UTF-8
import gc
import json
import os
import re
import requests
import queue
import threading
import redis
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from threading import Lock

from spider.user_agent import MY_USER_AGENT
from spider.my_exception import FaildTooManyTimesException, CrawlCompletedException, RedisCanNotWork
from spider.utils import my_urljoin, get_func_name, get_sort_set_from_redis, rem_sort_set_from_redis, put_element_into_sort_set, LineProgress
from random import randint
from bs4 import BeautifulSoup


# TODO: 加上爬取间歇时间
class MySpider:
    """
    自己实现的用来构建自动化搜索引擎的爬取部分的爬虫类
    """

    def __init__(self, config_filepath, output_queue, complete_queue):
        self.__config_filepath__ = config_filepath
        self.__file_name_count__ = 0
        self.__file_name_count_lock__ = Lock()
        self.__config_json__ = self.__read_config_json_from_file__()
        self.__config__ = self.__read_config_from_file__()
        self.__max_queue_size__ = 2000
        self.__not_access_queue__ = queue.PriorityQueue(
            maxsize=self.__max_queue_size__)
        self.__not_access_queue_name__ = 'naqn'
        self.__queue_full_flag__ = False
        self.__queue_harf_flag__ = False
        self.__accessed_set__ = set()
        self.__cannot_access_set__ = set()
        self.__output_queue__ = output_queue
        self.__headers__ = {
            'Connection':
            'keep-alive',
            'Accept':
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language':
            'zh-CN,zh;q=0.9,en;q=0.8',
            'Cache-Control':
            'max-age=0',
            'Referer':
            'http://www.baidu.com/',
        }
        self.__filter_urls_set__ = set()
        self.__url_compile_re__ = re.compile(r'^https?:/{2}\w.+$')
        self.__request_failed_nums__ = 2
        self.__allow_domain_regex_list__ = list()
        self.__progressbar__ = LineProgress(
            title='爬取页面',
            total=self.__config__['number_of_pages_to_crawl'],
            width=80)
        self.__complete_queue__ = complete_queue

    def __save_attr__(self):
        return dict(
            __file_name_count__=self.__file_name_count__,
            __config__=self.__config__,
            __max_queue_size__=self.__max_queue_size__,
            __not_access_queue__=list(self.__not_access_queue__.queue),
            __not_access_queue_name__=self.__not_access_queue_name__,
            __queue_full_flag__=self.__queue_full_flag__,
            __queue_harf_flag__=self.__queue_harf_flag__,
            __accessed_set__=list(self.__accessed_set__),
            __cannot_access_set__=(self.__cannot_access_set__))

    def __load_attr__(self, attr_dict):
        for attr, value in attr_dict.items():
            if attr == '__not_access_queue__':
                value = [i.decode() for i in value]
                for item in value:
                    item = item.strip('()').split(',')
                    priority, url = int(
                        item[0].split('.')[0]), item[1].strip('\'')[2:]
                    self.__not_access_queue__.put((priority, url))
            elif attr in ['__accessed_set__', '__cannot_access_set__']:
                selfattr = getattr(self, attr)
                for item in value:
                    selfattr.add(item.decode())
            elif attr == '__config__':
                setattr(self, attr, value)
            elif attr in ['__file_name_count__', '__max_queue_size__']:
                setattr(self, attr, int(value))
            elif attr in ['__queue_full_flag__', ' __queue_harf_flag__']:
                if value.decode() == 'True':
                    setattr(self, attr, True)
                elif value.decode() == 'False':
                    setattr(self, attr, False)
            else:
                setattr(self, attr, value.decode())

    def set_redis_conn(self, redis_conn):
        self.__redis_conn__ = redis_conn

    def get_redis_conn(self):
        return self.__redis_conn__s

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
            config['allow_domains'] = self.__config_json__.get(
                'ALLOW_DOMAINS', [])

            # 需要过滤的域名 Type:list Default:[]
            config['filter_domains'] = self.__config_json__.get(
                'FILTER_DOMAINS', [])

            # 需要优先爬取的域名 Type:list Default:[]
            config['priority_domains'] = self.__config_json__.get(
                'PRIORITY_DOMIANS', [])

            # 需要爬取的页面数量 Type:int Default:None
            config['number_of_pages_to_crawl'] = self.__config_json__.get(
                'NUMBER_OF_PAGES_TO_CRAWL', None)
            try:
                if config['number_of_pages_to_crawl'] is not None:
                    config['number_of_pages_to_crawl'] = int(
                        config['number_of_pages_to_crawl'])
                if config['number_of_pages_to_crawl'] <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError('配置文件填写错误，NUMBER_OF_PAGES_TO_CRAWL必须为一个正数')

            # 爬取每个页面的超时时间 Type:int Default:None
            config['timeout_of_per_page'] = self.__config_json__.get(
                'TIMEOUT_OF_PER_PAGE', None)
            try:
                if config['timeout_of_per_page'] is not None:
                    config['timeout_of_per_page'] = int(
                        config['timeout_of_per_page'])
                if config['timeout_of_per_page'] <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError('配置文件填写错误，TIMEOUT_OF_PER_PAGE必须为一个正数')

            # 是否采用深度优先 Type:boolean Default:False
            config['is_depth_first'] = self.__config_json__.get(
                'IS_DEPTH_FIRST', False)

            # 是否采用广度优先 Type:boolean Default:False
            config['is_breadth_first'] = self.__config_json__.get(
                'IS_BREADTH_FIRST', False)

            if config['is_depth_first'] == True and config['is_breadth_first'] == True:
                raise ValueError(
                    '配置文件填写错误，IS_DEPTH_FIRST与IS_BREADTH_FIRST不能同时为True')
            if config['is_depth_first'] == False and config['is_breadth_first'] == False:
                config['is_depth_first'] = True

            # 线程池中线程数量 Type:int Default:机器CPU核心数*4
            config['threadpool_size'] = self.__config_json__.get(
                'THREADPOOL_SIZE',
                cpu_count() * 4)
            try:
                config['threadpool_size'] = int(config['threadpool_size'])
                if config['threadpool_size'] <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError('配置文件填写错误，THREADPOOL_SIZE必须为一个正数')

            # 爬取下来的html文件的存储路径 Type:string default:当前工作目录下的html文件夹下
            config['html_file_storage_path'] = self.__config_json__.get(
                'HTML_FILE_STORAGE_PATH',
                os.path.join(os.getcwd(), 'saved_html'))
            if not os.path.exists(config['html_file_storage_path']):
                os.makedirs(config['html_file_storage_path'])
            if not os.path.isdir(config['html_file_storage_path']):
                raise NotADirectoryError

            # TODO:数据库参数读取
            redis_configs = self.__config_json__.get('REDIS', None)
            if redis_configs:
                config['redis_host'] = redis_configs.get('HOST', 'localhost')
                config['redis_port'] = redis_configs.get('PORT', '6379')
                config['redis_db'] = redis_configs.get('DB', 0)
                config['redis_pwd'] = redis_configs.get('PASSWORD', None)
            else:
                config['redis_host'] = 'localhost'
                config['redis_port'] = 6379
                config['redis_db'] = 0

            backup_redis_configs = self.__config_json__.get(
                'BACKUP_REDIS', None)
            if backup_redis_configs:
                config['backup_redis_host'] = backup_redis_configs.get(
                    'HOST', 'localhost')
                config['backup_redis_port'] = backup_redis_configs.get(
                    'PORT', '6379')
                config['backup_redis_db'] = backup_redis_configs.get('DB', 1)
                config['backup_redis_pwd'] = backup_redis_configs.get(
                    'PASSWORD', None)
            else:
                config['backup_redis_host'] = 'localhost'
                config['backup_redis_port'] = 6379
                config['backup_redis_db'] = 1

            mysql_configs = self.__config_json__.get('MYSQL', None)
            if mysql_configs:
                config['mysql_host'] = mysql_configs.get('HOST', 'localhost')
                config['mysql_port'] = mysql_configs.get('PORT', '3306')
                config['mysql_user'] = mysql_configs.get('USER', 'root')
                config['mysql_pwd'] = mysql_configs.get('PASSWORD', None)
                config['mysql_db'] = mysql_configs.get('DB', 'search_engine')
            else:
                raise KeyError('缺少MySQL配置参数')

            config['stop_words_path'] = self.__config_json__.get(
                'STOP_WORDS_PATH', None)

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
            self.__allow_domain_regex_list__.append(
                re.compile('.*' + allow_domain + '.*'))

    # 验证url是否满足allow_domians
    def __verify_url_for_allow_domains__(self, url):
        if (len(self.__allow_domain_regex_list__) == 0):
            return True
        for regx in self.__allow_domain_regex_list__:
            if (regx.match(url)):
                return True
        return False

    # 页面抓取器
    def __request__(self,
                    url,
                    priority,
                    timeout=20,
                    user_agent=MY_USER_AGENT[randint(0,
                                                     len(MY_USER_AGENT) - 1)]):
        self.__headers__['User-Agent'] = user_agent
        # 请求预设的次数，若失败的次数达到预设的最大次数则将异常抛出
        # 若请求成功则跳出请求循环返回请求结果
        for i in range(self.__request_failed_nums__):
            try:
                req = requests.get(
                    url, headers=self.__headers__, timeout=timeout)
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

        content = req.content
        del url, priority, timeout, user_agent, req
        return content

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
        doc = BeautifulSoup(page, 'html.parser')
        url = MySpider.get_hostname(url)
        for i in doc.find_all('a'):
            try:
                new_url = i['href']

                # 过滤不需要的域名
                if new_url in self.__filter_urls_set__:
                    continue

                standard_url = self.__url_standardizator__(i['href'], url)
                if self.__url_analyzer__(standard_url):
                    if self.__queue_full_flag__:
                        put_element_into_sort_set(
                            self.__redis_conn__,
                            self.__not_access_queue_name__, standard_url,
                            priority)
                    else:
                        self.__not_access_queue__.put(
                            (priority, standard_url), block=False)
            except queue.Full:
                self.__queue_full_flag__ = True
            except KeyError:
                pass
            except Exception as e:
                # TODO:找出可能会发生的异常
                print(get_func_name(), ':', e)
        del doc, priority, page, url

    # 深度优先页面抓取器
    def __depth_first_getter__(self, priority, page, url):
        self.__page__getter__(priority + 1, page, url)

    # 广度优先页面抓取器
    def __breadth_first_getter__(self, priority, page, url):
        self.__page__getter__(priority - 1, page, url)

    # 线程创造器
    def __thread_creator__(self):
        return ThreadPoolExecutor(
            max_workers=self.__config__['threadpool_size'])

    def start(self):
        # TODO: 将优先队列中的url改为(优先级, url)的元组形式
        self.__read_begin_urls__()
        self.__read_need_filter_urls__()
        self.__read_allow_domain_urls__()
        executor = self.__thread_creator__()

        while True:
            try:
                url_tuple = self.__not_access_queue__.get()
                if self.__queue_full_flag__ and self.__not_access_queue__.qsize(
                ) < self.__max_queue_size__ / 2:
                    get_sort_set_from_redis(self.__redis_conn__,
                                            self.__not_access_queue_name__,
                                            int(self.__max_queue_size__ * 0.3),
                                            self.__not_access_queue__)
                    gc.collect()
                # 再次验证，以免重复爬取已爬取过的页面
                if url_tuple[1] in self.__accessed_set__:
                    continue

                future = executor.submit(
                    self.__request__, url_tuple[1], url_tuple[0],
                    self.__config__['timeout_of_per_page'])
                writed_page = future.result()

                # TODO:存储page
                self.__file_name_count_lock__.acquire()
                file_path = os.path.join(
                    self.__config__['html_file_storage_path'],
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
                self.__progressbar__.update(self.__file_name_count__)

                # 爬取完成判断
                if self.__file_name_count__ == self.__config__[
                        'number_of_pages_to_crawl']:
                    # raise CrawlCompletedException()
                    self.__output_queue__.put('mission_complete')
                    self.__progressbar__.finish()
                    self.__complete_queue__.put('complete')
                    return
            # 若捕获到失败次数过多异常则将此请求的url放入不可访问链接集中
            # 并且取消本次请求
            except FaildTooManyTimesException:
                self.__cannot_access_set__.add(url_tuple[1])
                future.cancel()
            except redis.exceptions.ConnectionError:
                print('与Redis的连接中断，程序无法继续运行')
                exit()
            except RedisCanNotWork as e:
                print('Redis无法正常工作，程序无法继续运行', e)
                exit()
            finally:
                del url_tuple
