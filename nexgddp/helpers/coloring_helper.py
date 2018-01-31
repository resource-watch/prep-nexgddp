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
from functools import reduce
from matplotlib.colors import LinearSegmentedColormap
import matplotlib.colors

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
        sorted_values = list(map(lambda color: f"#{color[5:7]}{color[3:5]}{color[1:3]}", [color_ramps[k] for k in stops]))
        logging.debug(f"sorted_values: {sorted_values}")
        stop_range = [stops[0], stops[-1]]
        logging.debug(f"stop_range: {stop_range}")
        norms = list(map(
            lambda stop: float(1/(stop_range[1]-stop_range[0]) * (stop-stop_range[1]) + 1),
            stops
        ))

        norms[0] = 0.0 # Sometimes a *really* small float appears
        norms[-1] = 1.0
        logging.debug(f"norms: {norms}")
        colormap = LinearSegmentedColormap.from_list("custom_color_ramp", list(zip(norms, sorted_values)))
        logging.debug(f"colormap: {colormap}")
        return colormap

    @staticmethod
    def colorize(input_filename, style):
        logging.debug(f"[QueryService] Coloring raster {input_filename}")
        in_matrix = None

        image_with_alpha = cv2.imread(input_filename, cv2.IMREAD_UNCHANGED);
        mask = np.array(image_with_alpha, copy = True).astype(float)
        mask[mask > 0] = 1.0
        #logging.debug(mask)
        in_matrix = cv2.imread(input_filename, cv2.IMREAD_GRAYSCALE)
        #logging.debug(in_matrix)
        # color_lut = ColoringHelper.style_to_lut(style)
        color_lut = ColoringHelper.style_to_colormap(style)
        if in_matrix is not None:
            #logging.debug(in_matrix.shape)
            im_color = color_lut(in_matrix)
            im_color_split = cv2.split(im_color)
            logging.debug("im_color_split:")
            logging.debug(im_color_split)
            logging.debug(len(im_color_split))
            logging.debug(mask)
            #logging.debug("Color image")
            #logging.debug(im_color_split)
            final_image = cv2.merge((im_color_split[0], im_color_split[1], im_color_split[2], mask))
            logging.debug(final_image)
            logging.debug(final_image.shape)
            cv2.imwrite(input_filename, final_image * 255)
            f = open(input_filename, 'rb')
            return send_file(
                io.BytesIO(f.read()),
                attachment_filename='tile.png',
                mimetype='image/png'
            )
        else:
            return None
