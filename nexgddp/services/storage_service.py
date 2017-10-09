import logging
import os
from google.cloud import storage
from flask import request
from nexgddp.services.redis_service import RedisService


client = storage.Client()

class StorageService(object):
    @staticmethod
    def upload_file(filename, layer, z, x, y):
        
        logging.debug(f'filename is: {filename}')

        bucket = client.get_bucket('gee-tiles')
        blob = bucket.blob(layer + '/' + z + '/' + x + '/' + y + '/' + 'tile.png')
        with open(filename, 'rb') as my_file:
            blob.upload_from_file(my_file)
            blob.make_public()
            RedisService.set(request.path, blob.public_url)
            os.remove(filename)
            return blob.public_url
