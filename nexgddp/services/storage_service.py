import logging

from google.cloud import storage

from nexgddp.services.redis_service import RedisService

client = storage.Client()


class StorageService(object):

    @staticmethod
    def make_tile_cache_key(layer, z, x, y, year, compare_year=None, dset_b=None):
        if compare_year is None and dset_b is None:
            return f"{layer}/{str(z)}/{str(x)}/{str(y)}/{str(year)}.png"
        dset_b_str = ""
        if dset_b is not None:
            dset_b_str = f"{dset_b}/"
        extra_params = f"{dset_b_str}{compare_year}/"
        return f"{layer}/{str(z)}/{str(y)}/{str(x)}/{extra_params}{str(year)}.png"

    @staticmethod
    def upload_file(filename, layer, z, x, y, year, compare_year, dset_b):
        cache_key = StorageService.make_tile_cache_key(layer, z, x, y, year, compare_year, dset_b)
        bucket = client.get_bucket('nexgddp-tiles')
        blob = bucket.blob(cache_key)

        logging.debug(f"cache_key: {cache_key}")
        logging.debug(f"blob.public_url: {blob.public_url}")
        with open(filename, 'rb') as tile_file:
            blob.upload_from_file(tile_file)
            blob.make_public()
            RedisService.set(cache_key, blob.public_url)
            return blob.public_url

    @staticmethod
    def delete_folder(layer):
        bucket = client.get_bucket('nexgddp-tiles')
        # blob = bucket.blob(layer+"/0/0/0/tile.png")
        list = bucket.list_blobs(prefix=layer)
        bucket.delete_blobs(list)
        # for blob in list:
        #     logging.debug(blob)
        #     blob.delete()
        logging.debug("Folder removed")
