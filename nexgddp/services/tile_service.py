import logging
import mercantile
# from pyproj import Proj, transform

class TileService(object):
    @staticmethod
    def get_bbox(x, y, z):
        coords = list(mercantile.bounds(x, y, z))
        # [
        #     -180,
        #     -85.0511287798066,
        #     180,
        #     85.0511287798066
        # ]

        result = {
            'lat': [coords[1], coords[3]],
            'lon': [coords[0], coords[2]]
        }
        
        return result
