# coding: UTF-8
from spider import MySpider
from spider import CrawlCompletedException
from data_process import DataCleaner
from data_process.tf_idf import _Redis, TFIDF
from threading import Thread
from data_process.my_exception import RedisConnFailedException
from eprogress import LineProgress, CircleProgress, MultiProgressManager
from time import sleep
from spider.utils import if_need_load_progress, load_progress, save_progress, get_progress_file_path, backup

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
    tfidf = TFIDF(cleaned_queue)

    if if_need_load_progress(get_progress_file_path()):
        _redis = load_progress(get_progress_file_path(), crawled_queue, cleaned_queue,
        complete_single_queue, mySpider, tfidf)
    else:
        try:
            _redis = _Redis(mySpider.config['redis_host'], mySpider.config['redis_port'],
                        mySpider.config['redis_db'], mySpider.config['redis_pwd'] or None)
        except RedisConnFailedException:
            print('无法连接Redis服务器，请确保Redis服务已经开启且配置填写正确')
            exit()
        mySpider.set_redis_conn(_redis.getRedisConn())
        tfidf.set_redis_conn(_redis.getRedisConn())

    spider_thread = Thread(target=mySpider.start)
    cleaner_thread = Thread(target=cleaner.work)
    tfidf_thread = Thread(target=tfidf.start)
    # 备份线程 每5分钟进行一次备份
    backup_thread = Thread(target=backup, args=(30 ,get_progress_file_path(), crawled_queue,
        cleaned_queue, complete_single_queue, mySpider, _redis.getRedisConf()), 
        kwargs={'redis_host':mySpider.config['backup_redis_host'],
        'redis_port':mySpider.config['backup_redis_port'],
        'redis_db':mySpider.config['backup_redis_db'],
        'redis_pwd':mySpider.config['backup_redis_pwd']})

    # 设置为守护线程，在主线程结束时结束
    spider_thread.setDaemon(True)
    cleaner_thread.setDaemon(True)
    tfidf_thread.setDaemon(True)
    backup_thread.setDaemon(True)

    cleaner_thread.start()
    spider_thread.start()
    tfidf_thread.start()
    backup_thread.start()

    spider_thread.join()
    print('\n爬取完成，正在清洗数据')
    cleaner_thread.join()
    print('正在计算TFIDF值')
    tfidf_thread.join()
    # print('TFIDF计算完成')

    print('程序结束')
