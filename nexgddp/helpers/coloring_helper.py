"""Color helper"""

import logging
import tempfile
import os
import io
#import lycon
import cv2
import numpy as np
from colour import Color
from flask import send_file

class ColoringHelper(object):
    @staticmethod
    def normalize(min, max):
        return f" - {min}) * 255)/({max} - {min})"

    @staticmethod
    def get_data_bounds(indicator):
        return {
            'tasavg': [264.848, 327.099]            
        }.get(indicator, [0, 255])

    @staticmethod
    def colorize(input_filename, color_ramp_name = None, invert = False):
        logging.debug(f"[QueryService] Coloring raster {input_filename} with ramp {color_ramp_name}")
        in_matrix = cv2.imread(input_filename, cv2.IMREAD_GRAYSCALE)
        if invert:
            in_matrix = 255 - in_matrix
        color_ramp = ColoringHelper.get_color_ramp(color_ramp_name)
        logging.debug(f"color_ramp: {color_ramp}")
        if color_ramp or color_ramp == 0:
            im_color = cv2.applyColorMap(in_matrix, color_ramp)
            cv2.imwrite(input_filename, im_color)
        f = open(input_filename, 'rb')
        return send_file(
            io.BytesIO(f.read()),
            attachment_filename='tile.png',
            mimetype='image/png'
        )
    
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
