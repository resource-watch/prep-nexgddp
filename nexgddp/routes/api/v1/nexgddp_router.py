"""API ROUTER"""

import logging

from flask import jsonify, Blueprint
from nexgddp.routes.api import error
from nexgddp.validators import validate_greeting
from nexgddp.middleware import set_something
from nexgddp.serializers import serialize_greeting
from nexgddp.services import query_service
import json
import CTRegisterMicroserviceFlask

nexgddp_endpoints = Blueprint('nexgddp_endpoints', __name__)

@nexgddp_endpoints.route('/indicators/<scenario>/<model>/<year>/<indicator>', strict_slashes=False, methods=['GET'])
def get_raster(scenario, model, year):
    return nil
