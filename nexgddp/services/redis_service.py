"""Redis Service"""
import redis

from nexgddp.config import SETTINGS
from nexgddp.errors import RedisError

r = None
if SETTINGS.get('redis').get('url') is not None:
    r = redis.StrictRedis.from_url(url=SETTINGS.get('redis').get('url'))


class RedisService(object):

    @staticmethod
    def get(layer):
        if r is None:
            raise RedisError(status=500, message="Redis server not configured")
        text = r.get(layer)
        if text is not None:
            return text
        return None

    @staticmethod
    def set(key, value):
        if r is None:
            raise RedisError(status=500, message="Redis server not configured")
        return r.set(key, value)

    @staticmethod
    def expire_layer(layer):
        if r is None:
            raise RedisError(status=500, message="Redis server not configured")
        for key in r.scan_iter("*" + layer + "*"):
            r.delete(key)
