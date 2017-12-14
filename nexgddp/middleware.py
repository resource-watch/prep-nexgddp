"""MIDDLEWARE"""

from functools import wraps
from flask import request, redirect

from nexgddp.routes.api import error
from nexgddp.services.geostore_service import GeostoreService
from nexgddp.services.redis_service import RedisService
from nexgddp.services.layer_service import LayerService
from nexgddp.services.dataset_service import DatasetService
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


def get_year(func):
    """Get geodata"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        year = request.args.get('year', None)
        kwargs["year"] = year
        logging.debug(f"year: {year}")
        if not year:
            kwargs["year"] = None
            return func(*args, **kwargs)
        return func(*args, **kwargs)
    return wrapper

def get_tile_attrs(func):
    """Get style"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        layer_object = kwargs.get('layer_object', None)
        layer_config = layer_object.get('layerConfig', None)
        dataset = layer_object.get('dataset', None)
        logging.debug(f'dataset: {dataset}')
        
        logging.debug('Obtaining style')
        layer_style = layer_config.get('color_ramp')
        logging.debug(f'style: {layer_style}')
        kwargs["style"] = layer_style
        
        logging.debug('Obtaining year')
        # year = layer_config.get('year')
        # logging.debug(f'year: {year}')
        # kwargs["year"] = year

        logging.debug('Obtaining indicator')
        indicator = layer_config.get('indicator')
        logging.debug(f'indicator: {indicator}')
        kwargs["indicator"] = indicator

        
        logging.debug('Obtaining scenario and model')
        dataset_object = DatasetService.get(dataset)
        tablename = dataset_object.get('tableName', None)
        logging.debug(f'tablename: {tablename}')
        kwargs['model'] = tablename.split('/')[1]
        kwargs['scenario'] = tablename.split('/')[0]
        del kwargs['layer_object']
        return func(*args, **kwargs)
    return wrapper

# def exist_tile(func):
#     """Gets tile from cache"""
#     @wraps(func)
#     def wrapper(*args, **kwargs):
#         url = RedisService.get(request.path)
#         if url is None:
#             return func(*args, **kwargs)
#         else:
#             return redirect(url)
#     return wrapper

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

def tile_exists(func):
    """Checks if the tile exists in the cache"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.info("[Middleware] Checking if tile exists in cache")
        logging.debug(f'request.path: {request.path}')
        url = RedisService.get(request.path)
        logging.debug(f'url: {url}')
        if url is None:
            logging.debug("No tile found in cache")
            return func(*args, **kwargs)
        else:
            logging.debug("Tile found, redirecting")
            return redirect(url)
    return wrapper

def is_microservice(func):
    """Get geodata"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        logging.debug("Checking microservice user")        
        logged_user = json.loads(request.args.get("loggedUser", None))
        if logged_user.get("id") == "microservice":
            logging.debug("is microservice");
            return func(*args, **kwargs)
        else:
            return error(status=403, detail="Not authorized")
    return wrapper
