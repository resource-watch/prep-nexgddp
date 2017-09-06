"""API ROUTER"""

import logging

from flask import jsonify, request, Blueprint
from nexgddp.routes.api import error
from nexgddp.services.query_service import QueryService
from nexgddp.errors import SqlFormatError
from CTRegisterMicroserviceFlask import request_to_microservice

nexgddp_endpoints = Blueprint('nexgddp_endpoints', __name__)


@nexgddp_endpoints.route('/indicators/<scenario>/<model>/<year>/<indicator>', strict_slashes=False, methods=['GET'])
def get_raster(scenario, model, year):
    return None


@nexgddp_endpoints.route('/query/<dataset_id>', methods=['POST'])
def query(dataset_id):
    """NEXGDDP QUERY ENDPOINT"""
    logging.info('[ROUTER] Doing Query of dataset '+dataset_id)

    # Get and deserialize
    dataset = request.get_json().get('data', None)
    table_name = dataset.get('attributes').get('tableName')
    scenario, model, indicator = table_name.rsplit('/')
    sql = request.args.get('sql', None) or request.get_json().get('sql', None)

    # convert
    try:
        _, json_sql = QueryService.convert(sql)
    except SqlFormatError as e:
        logging.error(e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail='Generic Error')

    # query
    latitude = json_sql.get('where', None).get('lat', None)
    longitude = json_sql.get('where', None).get('long', None)
    year = json_sql.get('where', None).get('year', None)

    # @TODO -> doing query
    response = {}  # get rid of this
    if latitude or longitude:
        pass
        # call method 1 -> method1(scenario, model, indicator, lat, long, year)
    else:
        pass
        # call method 2 -> method2(scenario, model, indicator, year)

    return response, 200


@nexgddp_endpoints.route('/fields/<dataset_id>', methods=['POST'])
def fields(dataset_id):
    """NEXGDDP FIELDS ENDPOINT"""
    logging.info('[ROUTER] Getting fields of dataset'+dataset_id)

    # Get and deserialize
    dataset = request.get_json().get('data', None)
    table_name = dataset.get('attributes').get('tableName')
    scenario, model, indicator = table_name.rsplit('/')
    sql = request.args.get('sql', None) or request.get_json().get('sql', None)

    # @TODO -> get fields from method

    return jsonify(data=fields), 200


@nexgddp_endpoints.route('/rest-datasets/nexgddp', methods=['POST'])
def register_dataset():
    """NEXGDDP REGISTER ENDPOINT"""
    logging.info('Registering new NEXGDDP Dataset')

    # Get and deserialize
    table_name = request.get_json().get('connector').get('table_name')
    scenario, model, indicator = table_name.rsplit('/')

    # @TODO -> validate dataset
    if True:
        status = 1
    else:
        status = 2

    config = {
        'uri': '/dataset/'+request.get_json().get('connector').get('id'),
        'method': 'PATCH',
        'body': {'status': status}
    }
    response = request_to_microservice(config)
    return jsonify(response), 200
