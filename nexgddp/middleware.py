"""MIDDLEWARE"""

from functools import wraps
from flask import request

from nexgddp.routes.api import error
from nexgddp.services.geostore_service import GeostoreService
from nexgddp.services.redis_service import RedisService
from nexgddp.services.layer_service import LayerService
from nexgddp.errors import GeostoreNotFound, InvalidCoordinates, LayerNotFound

import logging

def get_bbox_by_hash(func):
    """Get geodata"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        geostore = request.args.get('geostore', None)
        if not geostore:
            bbox = []
        else:
            try:
                _, bbox = GeostoreService.get(geostore)
            except GeostoreNotFound:
                return error(status=404, detail='Geostore not found')

        kwargs["bbox"] = bbox
        return func(*args, **kwargs)
    return wrapper


def get_latlon(func):
    """Get geodata"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        geostore = request.args.get('geostore', None)
        lat = request.args.get('lat', None)
        lon = request.args.get('lon', None)
        if not geostore and not (lat and lon):
            kwargs["bbox"] = None
            return func(*args, **kwargs)
        if not geostore:
            bbox = [lat, lon]
            kwargs["bbox"] = bbox
        return func(*args, **kwargs)
    return wrapper


def get_style(func):
    """Get style"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        style = request.args.get('style', None)
        logging.debug(f"style: {style}")
        kwargs["style"] = style
        return func(*args, **kwargs)
    return wrapper

def exist_tile(func):
    """Gets tile from cache"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        url = RedisService.get(request.path)
        if url is None:
            return func(*args, **kwargs)
        else:
            return redirect(url)
    return wrapper

def get_layer(func):
    """Get geodata"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.debug('Getting layer')
        layer = request.view_args.get('layer', None)
        logging.debug(f'layer: {layer}')
        layer_object = LayerService.get(layer)
        logging.debug(f'layer_object: {layer_object}')
        kwargs["layer_object"] = layer_object
        return func(*args, **kwargs)
    return wrapper
