from spider import MySpider
from spider import CrawlCompletedException
import sys



if  __name__ == '__main__':
    mySpider = MySpider(sys.argv[1])
    try:
        mySpider.start()
    except CrawlCompletedException as e:
        pass
    print('爬取完成')
    