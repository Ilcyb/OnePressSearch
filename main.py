from spider import MySpider
from spider import CrawlCompletedException
from data_process import DataCleaner
from threading import Thread

import queue
import sys



if  __name__ == '__main__':
    crawled_queue = queue.Queue()
    cleaned_queue = queue.Queue()
    mySpider = MySpider(sys.argv[1], crawled_queue)
    cleaner = DataCleaner(crawled_queue, cleaned_queue)

    spider_thread = Thread(target=mySpider.start)
    cleaner_thread = Thread(target=cleaner.work)
    cleaner_thread.start()
    spider_thread.start()
    cleaner_thread.join()
    print('页面爬取完成')
    crawled_queue.join()
    spider_thread.join()
    print('页面清理完成')

    print('程序结束')
