class CantDecodeException(Exception):
    pass


class CleanFailedException(Exception):
    pass


class RedisConnFailedException(Exception):
    pass


class NoTitleOrNoUrlException(Exception):
    pass

class MySQLConnFailedException(Exception):
    pass

class DuplicateUrlException(Exception):
    pass