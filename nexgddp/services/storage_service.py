import logging
import os
from google.cloud import storage
from flask import request
from nexgddp.services.redis_service import RedisService


client = storage.Client()

class StorageService(object):
    @staticmethod
    def upload_file(filename, layer, z, x, y, year, compare_year, dset_b):
        
        if compare_year or dset_b:
            os.remove(filename)
            return None
        logging.debug(f'filename is: {filename}')

        bucket = client.get_bucket('gee-tiles')
        blob = bucket.blob(layer + '/' + z + '/' + x + '/' + y + '/' + str(year) + '.png')
        with open(filename, 'rb') as tile_file:
            blob.upload_from_file(tile_file)
            blob.make_public()
            RedisService.set(request.path + '_' + str(year), blob.public_url)
            os.remove(filename)
            return blob.public_url

    @staticmethod
    def delete_folder(layer): 
        bucket = client.get_bucket('gee-tiles')
        #blob = bucket.blob(layer+"/0/0/0/tile.png")
        list = bucket.list_blobs(prefix=layer)
        for blob in list:
            logging.debug(blob)
            blob.delete()
        logging.debug("Folder removed")
