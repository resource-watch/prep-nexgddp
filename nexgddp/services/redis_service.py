"""Redis Service"""
import redis

from nexgddp.config import SETTINGS

r = redis.StrictRedis.from_url(url=SETTINGS.get('redis').get('url'))


class RedisService(object):

    @staticmethod
    def get(layer):
        text = r.get(layer)
        if text is not None:
            return text
        return None

    @staticmethod
    def set(key, value):
        return r.set(key, value)

    @staticmethod
    def expire_layer(layer):
        for key in r.scan_iter("*" + layer + "*"):
            r.delete(key)
