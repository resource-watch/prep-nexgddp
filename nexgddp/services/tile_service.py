import logging
import mercantile

class TileService(object):
    @staticmethod
    def get_bbox(x, y, z):
        return list(mercantile.bounds(x, y, z))
