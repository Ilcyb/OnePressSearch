import sys
import redis
import abc
import re
import threading
import os
import json
import traceback
import pickle

from .my_exception import RedisCanNotWork
from data_process.tf_idf import _Redis
from time import sleep


def my_urljoin(first_url, second_url):
    return first_url.rstrip('/') + '/' + second_url.lstrip('./')


# FIXME: 修复只会显示该函数名的问题
def get_func_name():
    return sys._getframe().f_code.co_name
    

def get_sort_set_from_redis(redis_conn, set_name, num, queue):
    try:
        priority_set_list = redis_conn.zrevrange(set_name, 0, num-1, withscores=True)
        if rem_sort_set_from_redis(redis_conn, set_name, num):
            for priority_set in [(i[1], i[0].decode()) for i in priority_set_list]: # 调换url和priority的顺序
                queue.put(priority_set)
        else:
            return False
    except redis.exceptions.ConnectionError:
        raise
    except Exception as e:
        raise RedisCanNotWork(e)
    else:
        return True

def rem_sort_set_from_redis(redis_conn, set_name, num):
    try:
        redis_conn.zremrangebyrank(set_name, -num, -1)
    except redis.exceptions.ConnectionError:
        raise
    except Exception as e:
        raise RedisCanNotWork(e)
    else:
        return True

def put_element_into_sort_set(redis_conn, set_name, element_value, element_priority):
    try:
        if redis_conn.zscore(set_name, element_value) == None:
            redis_conn.zadd(set_name, element_value, element_priority)
    except redis.exceptions.ConnectionError:
        raise
    except Exception as e:
        raise RedisCanNotWork(e)
    else:
        return True


CLEAR_TO_END = "\033[K"
UP_ONE_LINE = "\033[F"


class ProgressBar(object, metaclass=abc.ABCMeta):
    def __init__(self, width=25, title=''):
        self.width = width
        self.title = ProgressBar.filter_str(title)
        self._lock = threading.Lock()

    @property
    def lock(self):
        return self._lock

    @abc.abstractmethod
    def update(self, progress=0):
        pass

    @staticmethod
    def filter_str(pending_str):
        """去掉字符串中的\r、\t、\n"""
        return re.sub(pattern=r'\r|\t|\n', repl='', string=pending_str)


class LineProgress(ProgressBar):
    def __init__(self, total=100, symbol='#', width=25, title=''):
        """
         @param total : 进度总数
         @param symbol : 进度条符号
         @param width : 进度条展示的长度
         @param title : 进度条前面展示的文字
        """
        super(LineProgress, self).__init__(width=width, title=title)
        self.total = total
        self.symbol = symbol
        self._current_progress = 0

    def update(self, progress=0):
        """
        @param progress : 当前进度值
        """
        with self.lock:
            if progress > 0:
                self._current_progress = progress
            sys.stdout.write('\r' + CLEAR_TO_END)
            hashes = '#' * int(self._current_progress / self.total * self.width)
            spaces = ' ' * (self.width - len(hashes))
            sys.stdout.write("\r%s:[%s] %d/%d" % (self.title, hashes + spaces, self._current_progress, self.total))

    def finish(self):
        self.update(self.total)


def get_progress_file_path():
    return os.path.join(os.path.dirname(__file__), '.session')

def save_progress(progress_file, crawled_queue, cleaned_queue, 
    complete_single_queue, spider, redis_conf, **kwargs):
    try:
        redis_conn = _Redis(host=kwargs['redis_host'], port=kwargs['redis_port'],
         db=kwargs['redis_db'], password=kwargs['redis_pwd']).getRedisConn()

        crawled_queue_list = list(crawled_queue.queue)
        cleaned_queue_list = list(cleaned_queue.queue)
        complete_single_queue_list = list(complete_single_queue.queue)
        spider_progree = spider.__save_attr__()

        # 清除上次保存的进度
        redis_conn.flushdb()

        for key in list(locals()):
            if key in ['crawled_queue_list', 'cleaned_queue_list', 'complete_single_queue_list']:
                for value in locals()[key]:
                    redis_conn.rpush(key, value)
        
        for k, v in spider_progree.items():
            redis_conn.rpush('spider_attr', k)
            if k in ['__not_access_queue__', '__accessed_set__', '__cannot_access_set__']:
                for item in v:
                    redis_conn.rpush(k, item)
            elif k == '__config__':
                config_byte = pickle.dumps(v)
                redis_conn.set('spider_configs', config_byte)
            else:
                redis_conn.set(k, v)

        for conf in redis_conf:
            redis_conn.rpush('redis_conf', conf)
        
        progress_json = {
            'progress': True,
            'redis_host': kwargs['redis_host'],
            'redis_port': kwargs['redis_port'],
            'redis_db': kwargs['redis_db'],
            'redis_pwd': kwargs['redis_pwd']
        }

        with open(progress_file, 'w', encoding='utf-8') as save_session_file:
            json.dump(progress_json, save_session_file)
        
    except redis.exceptions.ConnectionError:
        print('无法连接进度存储数据库，进度存储失败')
    except Exception as e:
        print('发生了未处理的错误，无法保存程序进度', e)


def load_progress(progress_file, crawled_queue, cleaned_queue, complete_single_queue, spider, tfidf):
    try:
        with open(progress_file, 'r') as load_session_file:
            progress_json = json.load(load_session_file)
            
        redis_conn = _Redis(host=progress_json['redis_host'], port=progress_json['redis_port'],
         db=progress_json['redis_db'], password=progress_json['redis_pwd']).getRedisConn()
        
        for redis_list in ['crawled_queue_list', 'cleaned_queue_list', 'complete_single_queue_list']:
            all_item_list = redis_conn.lrange(redis_list, 0, -1)
            for key in list(locals()):
                if redis_list[:-5] == key:
                    current_queue = locals()[key]
                    for item in all_item_list:
                        current_queue.put(item.decode()) # redis返回的数据都是byte类型的，需要decode
        
        spider_attr_dict = dict()
        for attr in redis_conn.lrange('spider_attr', 0, -1):
            attr = attr.decode()
            if attr in ['__not_access_queue__', '__accessed_set__', '__cannot_access_set__']:
                spider_attr_dict[attr] = redis_conn.lrange(attr, 0, -1)
            elif attr == '__config__':
                spider_attr_dict[attr] = pickle.loads(redis_conn.get('spider_configs'))
            else:
                spider_attr_dict[attr] = redis_conn.get(attr) # spider的属性的decode交给spider.__load_attr__自己去做
        spider.__load_attr__(spider_attr_dict)

        redis_conf = [conf.decode() for conf in redis_conn.lrange('redis_conf', 0, -1)]
        old_redis = _Redis(redis_conf[0], int(redis_conf[1]), 
                int(redis_conf[2]), None if redis_conf[3] == 'None' else redis_conf[3])
        spider.set_redis_conn(old_redis.getRedisConn())
        tfidf.set_redis_conn(old_redis.getRedisConn())
        return old_redis
        
    except redis.exceptions.ConnectionError:
        print('无法连接进度存储数据库，进度加载失败')
    except Exception as e:
        print('发生了未处理的错误，无法加载程序进度', e)
        print(traceback.format_exc())

def if_need_load_progress(progress_file):
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            progress_json = json.load(f)
        if progress_json.get('progress') == True:
            return True
        else:
            return False
    except FileNotFoundError:
        return False
    except KeyError:
        return False

def backup(minute, progress_file, crawled_queue, cleaned_queue, 
    complete_single_queue, spider, redis_conf,**kwargs):
    while True:
        sleep(minute * 60)
        save_progress(progress_file, crawled_queue, cleaned_queue,
        complete_single_queue, spider, redis_conf, **kwargs)
