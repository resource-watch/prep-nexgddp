"""GDAL Helper"""

from osgeo import gdal, gdalnumeric
import logging

class GdalHelper(object):
    @staticmethod
    def calc_stats(gdal_dataset):
        first_band = gdal_dataset.GetRasterBand(1)
        if first_band is None:
            logging.error("No available data in raster")
        stats = first_band.GetStatistics(True, True)
        return {
            "min": stats[0],
            "max": stats[1],
            "avg": stats[2],
            "stdev": stats[3]
        }
