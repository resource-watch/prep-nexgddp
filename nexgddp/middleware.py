"""MIDDLEWARE"""

from functools import wraps
from flask import request

from nexgddp.routes.api import error
from nexgddp.services.geostore_service import GeostoreService
from nexgddp.errors import GeostoreNotFound


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
