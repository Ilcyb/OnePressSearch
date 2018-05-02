from json import load
import redis
import MySQLdb
import _mysql_exceptions
import hashlib


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
            raise

    def getRedisConn(self):
        return self._conn

    def getRedisConf(self):
        return [self._host, self._port, self._db, self._password]


def get_redis_conn(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        configs = load(f)
    try:
        redis_config = configs['REDIS']
        _redis = _Redis(
            redis_config.get('HOST', '127.0.0.1'),
            redis_config.get('PORT', 6379), redis_config.get('DB', 0),
            redis_config.get('PASSWORD', None))
        redis_conn = _redis.getRedisConn()
        return redis_conn
    except KeyError:
        return _Redis('127.0.0.1', 6379, 0).getRedisConn()
    except redis.exceptions.ConnectionError as e:
        print('无法连接配置为', _redis.getRedisConf(),
              '的redis服务器，请检查redis服务器是否开启，连接参数是否填写正确')
    except Exception as e:
        print('发生了未处理的错误', e)


class _MySQL(object):
    def __init__(self, host, port, db, user, password):
        self._host, self._port, self._db, self._user, self._password = \
            host, int(port), db, user, password
        try:
            self._db = MySQLdb.connect(host=host, port=int(port), db=db, user=user, passwd=password, charset='utf8')
        except _mysql_exceptions.OperationalError as e:
            raise e

    def _re_open_connect(self):
        self._db.close()
        self._db = MySQLdb.connect(host=self._host, port=int(self._port),
                                   db=self._db, user=self._user, passwd=self._password)

    def getMySQLConn(self):
        return self._db

    def getMySQLConf(self):
        return [self._host, self._port, self._db, self._user, self._password]


def get_MySQL_conn(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        configs = load(f)
    try:
        mysql_config = configs['MYSQL']
        _mysql = _MySQL(
            mysql_config.get('HOST', '127.0.0.1'),
            mysql_config.get('PORT', 3306),
            mysql_config.get('DB', 'search_engine'),
            mysql_config.get('USER', 'root'),
            mysql_config.get('PASSWORD', None))
        mysql_conn = _mysql.getMySQLConn()
        return mysql_conn
    except KeyError:
        return _MySQL('127.0.0.1', 3306, 'search_engine', 'root', 'root').getMySQLConn()
    except redis.exceptions.ConnectionError as e:
        print('无法连接配置为', _mysql.getMySQLConf(),
              '的MySQL服务器，请检查MySQL服务器是否开启，连接参数是否填写正确')
    except Exception as e:
        print('发生了未处理的错误', e)


def mymd5(str):
    str = str.encode()
    md5 = hashlib.md5()
    md5.update(str)
    return md5.hexdigest()


def find_keyword(content, keyword_list, threshold=80):
    keyword_list = sorted(keyword_list, key=lambda s:len(s), reverse=True)
    for keyword in keyword_list:
        index = content.find(keyword)
        if(index == -1):
            pass
        else:
            left = index - threshold if index >= threshold else 0
            right = index + threshold
            return content[left:right]
