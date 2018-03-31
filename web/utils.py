from json import load
from sys import path
import redis


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
