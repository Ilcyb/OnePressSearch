# coding: UTF-8
from spider import MySpider
from spider import CrawlCompletedException
from data_process import DataCleaner
from data_process.tf_idf import _Redis, TFIDF
from threading import Thread
from data_process.my_exception import RedisConnFailedException
from eprogress import LineProgress, CircleProgress, MultiProgressManager
from time import sleep

import queue
import sys

# TODO: 改善内存使用效率
# TODO: 意外退出时保留进度

if  __name__ == '__main__':
    crawled_queue = queue.Queue()
    cleaned_queue = queue.Queue()
    complete_single_queue = queue.Queue()
    mySpider = MySpider(sys.argv[1], crawled_queue, complete_single_queue)
    cleaner = DataCleaner(crawled_queue, cleaned_queue, complete_single_queue)

    try:
        redis = _Redis(mySpider.config['redis_host'], mySpider.config['redis_port'],
                    mySpider.config['redis_db'], mySpider.config['redis_pwd'] or None)
    except RedisConnFailedException:
        print('无法连接Redis服务器，请确保Redis服务已经开启且配置填写正确')
        exit()
    
    mySpider.get_redis_conn(redis.getRedisConn())
    tfidf = TFIDF(cleaned_queue, redis.getRedisConn())

    spider_thread = Thread(target=mySpider.start)
    cleaner_thread = Thread(target=cleaner.work)
    tfidf_thread = Thread(target=tfidf.start)

    cleaner_thread.start()
    spider_thread.start()
    tfidf_thread.start()

    spider_thread.join()
    print('\n爬取完成，正在清洗数据')
    cleaner_thread.join()
    print('正在计算TFIDF值')
    tfidf_thread.join()
    # print('TFIDF计算完成')

    print('程序结束')
