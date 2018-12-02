import redis

from log import tolog


class RedisObj(object):
    def __init__(self, host, port, db):
        self.redis_conn = self.connect(host, port, db)

    def connect(self, host, port, db):
        return redis.Redis(host=host, port=port, db=db)
