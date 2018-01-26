import json
import os
import sys
from multiprocessing import cpu_count


class MySpider:
    """
    自己实现的用来构建自动化搜索引擎的爬取部分的爬虫类
    """
    def __init__(self,config_filepath):
        self.__config_filepath__ = config_filepath
        self.__config_json__ = self.__read_config_json_from_file__()
        self.__config__ = self.__read_config_from_file__()

    def __read_config_json_from_file__(self):
        try:
            with open(self.__config_filepath__, 'r') as f:
                config_json = json.loads(f.read())
                return config_json
        except FileNotFoundError:
            print('配置文件不存在，程序将结束')
            exit()

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
                pass #TODO:选择默认爬取方式
            
            # 线程池中线程数量 Type:int Default:机器CPU核心数*4
            config['threadpool_size'] = self.__config_json__.get('THREADPOOL_SIZE', cpu_count() * 4)
            try:
                config['threadpool_size'] = int(config['threadpool_size'])
                if config['threadpool_size'] <= 0:
                    raise ValueError
            except ValueError:
                raise ValueError('配置文件填写错误，THREADPOOL_SIZE必须为一个正数')

            # 爬取下来的html文件的存储路径 Type:string default:当前工作目录下的html文件夹下
            config['html_file_storage_path'] = self.__config_json__.get('HTML_FILE_STORAGE_PATH', os.path.join(os.getcwd(), 'saved_html'))
            if not os.path.exists(config['html_file_storage_path']):
                os.mkdir(config['html_file_storage_path'])
            if not os.path.isdir(config['html_file_storage_path']):
                raise NotADirectoryError
            
            #TODO:数据库参数读取

            return config

        except (KeyError ,ValueError, NotADirectoryError) as e:
            print(e)
            exit()

    def __getattr__(self, key):
        if key == 'config' or key == 'configs':
            return self.__config__

if __name__ == '__main__':
    mySpider = MySpider(sys.argv[1])
    print(mySpider.config)
