"""Color helper"""
import logging

import cv2
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

from nexgddp.errors import CoverageNotFound


class ColoringHelper(object):
    @staticmethod
    def normalize(min, max):
        return f" - {min}) * 255)/({max} - {min})"

    @staticmethod
    def get_data_bounds(style):
        # Minimum and maximum values from the style
        logging.debug("Obtaining data bounds")
        values = sorted(list(map(lambda stop: stop['stop'], style)))
        logging.debug(values)
        return [values[0], values[-1]]

    @staticmethod
    def style_to_colormap(style):
        color_ramps = dict(list(map(lambda stop: [float(stop['stop']), stop['color']], style)))
        logging.debug(f"color_ramps: {color_ramps}")
        stops = sorted([k for k in color_ramps])
        logging.debug(f"stops: {stops}")
        sorted_values = list(
            map(lambda color: f"#{color[5:7]}{color[3:5]}{color[1:3]}", [color_ramps[k] for k in stops]))
        logging.debug(f"sorted_values: {sorted_values}")
        stop_range = [stops[0], stops[-1]]
        logging.debug(f"stop_range: {stop_range}")
        norms = list(map(
            lambda stop: float(1 / (stop_range[1] - stop_range[0]) * (stop - stop_range[1]) + 1),
            stops
        ))
        norms[0] = 0.0  # Sometimes a *really* small float appears
        norms[-1] = 1.0
        logging.debug(f"norms: {norms}")
        colormap = LinearSegmentedColormap.from_list("custom_color_ramp", list(zip(norms, sorted_values)))
        logging.debug(f"colormap: {colormap}")
        return colormap

    @staticmethod
    def colorize(input_filename, style):
        logging.debug(f"[ColoringHelper] Coloring raster {input_filename}")
        # It's important to set the correct flags to read the file,
        # lest not have correct dimensionality later
        in_matrix = cv2.imread(input_filename, cv2.IMREAD_GRAYSCALE)
        color_lut = ColoringHelper.style_to_colormap(style)
        if in_matrix is not None:
            im_color = color_lut(in_matrix)
            cv2.imwrite(input_filename, im_color * 255)
            logging.debug(input_filename)
        else:
            raise CoverageNotFound("No data for the supplied coordinates")
        return input_filename

    @staticmethod
    def blend_alpha(output_filename, mask_filename):
        logging.debug(f"[ColoringHelper] Blending alpha")
        output_raster = cv2.imread(output_filename, cv2.IMREAD_COLOR)
        output_mask = cv2.imread(mask_filename, cv2.IMREAD_GRAYSCALE)
        expanded_mask = np.expand_dims(output_mask, axis=-1)
        logging.debug(expanded_mask.shape)
        logging.debug(output_raster.shape)
        final_result = np.concatenate((output_raster, 255 - expanded_mask), axis=-1)
        cv2.imwrite(output_filename, final_result)
        logging.debug(f"final_result.shape: {final_result.shape}")
        return output_filename
