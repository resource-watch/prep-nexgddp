"""Color helper"""

import logging
import tempfile
import os
import lycon
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

            in_matrix = lycon.load(input_filename)
            logging.debug(f"in_matrix: {in_matrix}")
            logging.debug(f"shape: {in_matrix.shape}")

            ramp_function = ColoringHelper.get_color_ramp(color_ramp)

            logging.debug(f"ramp_function(3): {ColoringHelper.prtobyte(ramp_function(3))}")

            # We operate over a flattened view of the data - faster
            first_band = in_matrix[:,:,0].reshape(-1)
            logging.debug(f"first_band: {first_band}")
            logging.debug(f"first_band shape: {first_band.shape}")

            processed_data = []

            for i, v in enumerate(first_band):
                processed_data[i] = ramp_function(v)
            logging.debug(f"processed_data: {processed_data}")

            return output_filename

    @staticmethod
    def get_color_ramp(color_ramp):
        return {
                'spectral': np.vectorize(lambda x: list(Color('red').range_to(Color('green'), 256))[x].rgb)
        }.get(color_ramp, np.vectorize(lambda x: x))

    @staticmethod
    def prtobyte(vector):
        logging.debug("Transforming vector from prop to byte")
        logging.debug(f"vector: {vector}")
        to_byte_int = lambda v: int(v * 255)
        to_byte_int_v = np.vectorize(to_byte_int)
        return to_byte_int_v(vector)
