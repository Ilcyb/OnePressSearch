"""
author: ilcyb
~~~~~~~~~~~~~

自定义异常模块
"""


class FaildTooManyTimesException(Exception):
    """
    请求失败的次数达到了预设的最大次数
    """
    pass


class CrawlCompletedException(Exception):
    """
    爬取次数达到了预设的次数
    """
    pass


class RedisCanNotWork(Exception):
    pass