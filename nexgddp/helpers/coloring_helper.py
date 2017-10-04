"""Color helper"""

import logging
import tempfile
import os
#import lycon
import cv2
import numpy as np
from colour import Color


class ColoringHelper(object):
    @staticmethod
    def normalize(min, max):
        return f" - {min}) * 255)/({max} - {min})"

    @staticmethod
    def get_data_bounds(indicator):
        return {
            'tmax5day': [264.848, 327.099]
        }.get(indicator, [0, 255])

    @staticmethod
    def colorize(input_filename, color_ramp_name = None):
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as output_filename:
            in_matrix = cv2.imread(input_filename, cv2.IMREAD_GRAYSCALE)
            color_ramp = ColoringHelper.get_color_ramp(color_ramp_name)
            logging.debug(f"[QueryService] Coloring raster {input_filename} with ramp {color_ramp}")
            if color_ramp:
                im_color = cv2.applyColorMap(in_matrix, cv2.COLORMAP_SUMMER)
                cv2.imwrite(input_filename, im_color)
            return input_filename

    @staticmethod
    def get_color_ramp(color_ramp_name):
        return {
            'autumn':  cv2.COLORMAP_AUTUMN,
            'bone':    cv2.COLORMAP_BONE,
            'cool':    cv2.COLORMAP_COOL,
            'hot':     cv2.COLORMAP_HOT,
            'hsv':     cv2.COLORMAP_HSV,
            'jet':     cv2.COLORMAP_JET,
            'ocean':   cv2.COLORMAP_OCEAN,
            'pink':    cv2.COLORMAP_PINK,
            'rainbow': cv2.COLORMAP_RAINBOW,
            'spring':  cv2.COLORMAP_SPRING,
            'summer':  cv2.COLORMAP_SUMMER,
            'winter':  cv2.COLORMAP_WINTER
        }.get(color_ramp_name, None)
