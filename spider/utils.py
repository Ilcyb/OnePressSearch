import sys
import redis
import abc
import re
import threading

from .my_exception import RedisCanNotWork


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