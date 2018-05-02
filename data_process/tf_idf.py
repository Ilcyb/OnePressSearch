# coding: UTF-8
import jieba
import redis
import os

from math import log10
from .my_exception import RedisConnFailedException, CantDecodeException, \
                        NoTitleOrNoUrlException, MySQLConnFailedException, DuplicateUrlException
import MySQLdb
import _mysql_exceptions


class _Redis(object):
    def __init__(self, host, port, db, password=None):
        self._host, self._port, self._db, self._password = host, port, db, password
        if password:
            self._conn = redis.Redis(
                host=host, port=port, db=db, password=password)
        else:
            self._conn = redis.Redis(host=host, port=port, db=db)
        try:
            self._conn.ping()
        except redis.exceptions.ConnectionError as e:
            raise RedisConnFailedException(str(e))

    def getRedisConn(self):
        return self._conn

    def getRedisConf(self):
        return [self._host, self._port, self._db, self._password]


class _MySQL(object):
    def __init__(self, host, port, db, user, password):
        self._host, self._port, self._db, self._user, self._password = \
        host, int(port), db, user, password
        try:
            self._db = MySQLdb.connect(host=host, port=int(port), db=db, user=user, passwd=password, charset='utf8')
        except _mysql_exceptions.OperationalError as e:
            raise MySQLConnFailedException(str(e))

    def _re_open_connect(self):
        self._db.close()
        self._db = MySQLdb.connect(host=self._host, port=int(self._port),
         db=self._db, user=self._user, passwd=self._password)
    
    def getMySQLConn(self):
        return self._db

    def getMySQLConf(self):
        return [self._host, self._port, self._db, self._user, self._password]

class TFIDF(object):
    def __init__(self, input_queue):
        self._input_queue = input_queue
        self._stopWordList()
        jieba.setLogLevel(30)

    def set_redis_conn(self, redis_conn):
        self._redis_conn = redis_conn

    def set_mysql_conn(self, mysql_conn):
        self._mysql_conn = mysql_conn

    def _stopWordList(self,
                      stop_words_path=os.path.join(
                          os.path.dirname(__file__), 'stop_words.txt')):
        self._stop_words_set = set()
        with open(stop_words_path, 'r', encoding='utf-8') as stw_file:
            self._stop_words_set.add(stw_file.readline().strip())

    def _participle(self, str):
        seg_list = jieba.lcut_for_search(str)
        result = list()
        for word in seg_list:
            if word not in self._stop_words_set:
                result.append(word)
        return result

    def _read_content(self, path, coding='utf-8'):
        while True:
            try:
                page = open(path, encoding=coding)
                url = page.readline().strip()
                title = page.readline().strip()
                lines = page.readlines()
                s = ''
                for line in lines:
                    s += line
            except FileNotFoundError:
                raise FileNotFoundError(path + "don't exists")
            except UnicodeDecodeError:
                if coding == 'utf-8':
                    coding = 'gbk'
                else:
                    raise CantDecodeException(
                        path + "can't decode by utf-8 and gbk")
            else:
                return url, title, s
            finally:
                page.close()

    def _get_word_count_dict(self, words):
        word_count_dict = dict()
        for word in words:
            if word not in word_count_dict:
                word_count_dict[word] = 1
            else:
                word_count_dict[word] += 1
        return word_count_dict, len(words)

    def _write_info_2_redis(self, words_dict, total, url, title):
        if url and title:
            self._redis_conn.hset('url2title', url, title)
        else:
            raise NoTitleOrNoUrlException
        for word, count in words_dict.items():
            self._redis_conn.sadd('all_word_list', word)
            if not self._redis_conn.hexists(word, 'url_list'):
                self._redis_conn.hset(word, 'url_list', word + '_url_list')
                self._redis_conn.hset(word, 'tfidf_list', word + '_tfidf_list')
            self._redis_conn.rpush(
                self._redis_conn.hget(word, 'url_list'), url)
            tf = count / total
            self._redis_conn.rpush(
                self._redis_conn.hget(word, 'tfidf_list'), tf)

    def _compute_tfidf(self):
        total_words_set = self._redis_conn.smembers('all_word_list')
        total_doc_nums = len(total_words_set)
        for word in total_words_set:
            the_word_doc_nums = self._redis_conn.llen(
                self._redis_conn.hget(word, 'url_list')) + 1
            the_word_idf = log10(total_doc_nums / (the_word_doc_nums + 1))
            the_word_tf_list_name = self._redis_conn.hget(word, 'tfidf_list')
            for index in range(self._redis_conn.llen(the_word_tf_list_name)):
                the_tf = float(
                    self._redis_conn.lindex(the_word_tf_list_name, index))
                the_tf_idf = the_word_idf * the_tf
                self._redis_conn.lset(the_word_tf_list_name, index, the_tf_idf)

    def _write_content_2_mysql(self, url, content):
        from spider.utils import mymd5
        try:
            db_conn = self._mysql_conn.getMySQLConn()
            db_conn.begin()
            cur = db_conn.cursor()
            md5 = mymd5(url)
            if(cur.execute("""select id from url_hash where url_hash = %s""", (md5,)) != 0):
                raise DuplicateUrlException()
            cur.execute("""insert into url_hash(url_hash, content) values(%s, %s)""",
                        (md5, content))
        except _mysql_exceptions.OperationalError as e:
            db_conn.rollback()
            raise e
        else:
            db_conn.commit()
        finally:
            cur.close()

    def start(self):
        while True:
            try:
                avilable_path = self._input_queue.get()
                if avilable_path == 'mission_complete':
                    break
                url, title, content = self._read_content(avilable_path)
                self._write_content_2_mysql(url, content if len(content.strip()) != 0 else title)
                self._write_info_2_redis(
                    *self._get_word_count_dict(self._participle(content)), url,
                    title)
            except FileNotFoundError:
                pass
            except CantDecodeException:
                pass
            except NoTitleOrNoUrlException:
                pass
            except redis.exceptions.ConnectionError:
                print('与redis的连接中断')
            except _mysql_exceptions.OperationalError as e:
                print(e)
            except Exception as e:
                from traceback import print_exc
                print_exc()
                print('发生了未经处理的错误', e)
            finally:
                self._input_queue.task_done()
        try:
            self._compute_tfidf()
        except redis.exceptions.ConnectionError:
            print('与redis的连接中断')
            return
        except Exception as e:
            print('发生了未经处理的错误', e)
            return
        return
