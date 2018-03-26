# coding: UTF-8
from spider import MySpider
from spider import CrawlCompletedException
from data_process import DataCleaner
from data_process.tf_idf import _Redis, TFIDF
from threading import Thread
from data_process.my_exception import RedisConnFailedException

import queue
import sys



if  __name__ == '__main__':
    crawled_queue = queue.Queue()
    cleaned_queue = queue.Queue()
    mySpider = MySpider(sys.argv[1], crawled_queue)
    cleaner = DataCleaner(crawled_queue, cleaned_queue)

    try:
        redis = _Redis(mySpider.config['redis_host'], mySpider.config['redis_port'],
                    mySpider.config['redis_db'], mySpider.config['redis_pwd'] or None)
    except RedisConnFailedException:
        print('无法连接Redis服务器，请确保Redis服务已经开启且配置填写正确')
        exit()
    tfidf = TFIDF(cleaned_queue, redis.getRedisConn())

    spider_thread = Thread(target=mySpider.start)
    cleaner_thread = Thread(target=cleaner.work)
    tfidf_thread = Thread(target=tfidf.start)

    cleaner_thread.start()
    spider_thread.start()
    tfidf_thread.start()

    spider_thread.join()
    print('页面爬取完成')
    cleaner_thread.join()
    print('页面清理完成')
    tfidf_thread.join()
    print('TFIDF计算完成')

    print('程序结束')
