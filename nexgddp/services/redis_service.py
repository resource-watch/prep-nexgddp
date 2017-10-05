""" Redis service """

import redis
import json
import logging

from nexgddp.config import SETTINGS

r = redis.StrictRedis.from_url(url=SETTINGS.get('redis').get('url'))

class RedisService(object):

    @staticmethod
    def set(key, value):
        return r.set(key, value)
