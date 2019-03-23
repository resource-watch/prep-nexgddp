"""GDAL Helper"""

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

    @staticmethod
    def calc_histogram(gdal_dataset):
        first_band = gdal_dataset.GetRasterBand(1)
        if first_band is None:
            logging.error("No available data in raster")
        hist = first_band.GetDefaultHistogram(force=1)
        return {
            "min": hist[0],
            "max": hist[1],
            "buckets": hist[2],
            "st_histogram": hist[3]
        }
