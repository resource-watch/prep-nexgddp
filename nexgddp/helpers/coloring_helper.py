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
    def colorize(input_filename, color_ramp = 'spectral'):
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as output_filename:
            logging.debug(f"[QueryService] Coloring raster {input_filename} with ramp {color_ramp}")
            in_matrix = cv2.imread(input_filename, cv2.IMREAD_GRAYSCALE)
            im_color = cv2.applyColorMap(in_matrix, cv2.COLORMAP_JET)

            cv2.imwrite(input_filename, im_color)
            # lookup_table = ColoringHelper.get_color_lut(color_ramp)
            # logging.debug(f"LUT: {lookup_table}")

            # out_matrix = cv2.LUT(in_matrix, lookup_table)

            return input_filename
            
            # We operate over a flattened view of the data - faster

            

    @staticmethod
    def get_color_lut(color_ramp):
        coloring_function =  {
                'spectral': lambda x: list(Color('red').range_to(Color('green'), 256))[x].rgb
        }.get(color_ramp, lambda y: [y, y, y])

        out_list = list(map(
            lambda x: list(ColoringHelper.pr_to_byte(coloring_function(x))),
            list(range(256))
        ))
        out_arr = np.asarray(out_list, dtype=np.dtype('uint8'))
        return out_arr

    @staticmethod
    def pr_to_byte(vector):
        to_byte_int = lambda v: int(v * 255)
        to_byte_int_v = np.vectorize(to_byte_int)
        return to_byte_int_v(vector)
