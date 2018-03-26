"""
author: ilcyb
将爬取下来的html文件中的冗余信息去除
"""


from bs4 import BeautifulSoup
from .cx_extractor import CxExtractor
from .my_exception import CantDecodeException, CleanFailedException
from os import remove

class DataCleaner(object):
    """
    数据清理类
    将html文件中的无用信息清理掉，留下有用的标题、关键词、正文
    """
    def __init__(self, input_queue, output_queue):
        self.__input_queue__ = input_queue
        self.__output_queue__ = output_queue
        self.__extractor__ = CxExtractor()

    def __clean__(self, path):
        try:
            url, html_content = self.__extractor__.readHtml(path)
            title, keywords = self.__preprocess__(html_content) or ('', '')
            content = self.__extractor__.filter_tags(html_content)
            text = self.__extractor__.getText(content)
            with open(path, 'wb') as re_write:
                re_write.write(url.encode())
                re_write.write(title.encode() + b'\n')
                re_write.write(text.encode())
                re_write.write(keywords.encode())
            return path
        except FileNotFoundError as e:
            print(e)
            raise CleanFailedException()
        except CantDecodeException as e:
            print(e)
            raise CleanFailedException()

    def __preprocess__(self, content):
        doc = BeautifulSoup(content)
        keywords = str()
        description = str()
        title = str()
        try:
            keywords = doc.select('meta[name="keywords"]')[0]['content']
        except Exception:
            pass
        try:
            description = doc.select('meta[name="description"]')[0]['content']
        except Exception:
            pass
        try:
            title = doc.find('title').string
        except Exception:
            pass
        try:
            return ((title or '') ,(' ' + keywords or ' ') + (' ' + description or ' '))
        except Exception:
            return ('', '')

    def work(self):
        while True:
            try:
                avilable_path = self.__input_queue__.get()
                if avilable_path == 'mission_complete':
                    self.__output_queue__.put('mission_complete')
                    break
                avilable_path = self.__clean__(avilable_path)
                print('clean done:', avilable_path)
                self.__output_queue__.put(avilable_path)
            except CleanFailedException:
                remove(avilable_path)
            finally:
                self.__input_queue__.task_done()
        return
