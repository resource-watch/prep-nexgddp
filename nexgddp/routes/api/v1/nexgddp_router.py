"""API ROUTER"""

import logging

from flask import jsonify, request, Blueprint, Response
from nexgddp.routes.api import error
from nexgddp.services.query_service import QueryService
from nexgddp.errors import SqlFormatError
from CTRegisterMicroserviceFlask import request_to_microservice

nexgddp_endpoints = Blueprint('nexgddp_endpoints', __name__)

def generate_response(response):
    for chunk in response.iter_content(chunk_size=1024):
        yield chunk

def callback_to_dataset(body):
    config = {
        'uri': '/dataset/'+request.get_json().get('connector').get('id'),
        'method': 'PATCH',
        'body': body
    }
    return request_to_microservice(config)

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
    longitude = json_sql.get('where', None).get('long', None)  # Need to establish a syntax. 'long' not really appropriate, as it's a reserved word
    year = json_sql.get('where', None).get('year', None)

    # st_histogram?
    response = QueryService.get_raster_file(scenario, model, year, indicator)
    return (generate_response(response), response.headers['content-type'])


@nexgddp_endpoints.route('/fields/<dataset_id>', methods=['POST'])
def get_fields(dataset_id):
    """NEXGDDP FIELDS ENDPOINT"""
    logging.info('[ROUTER] Getting fields of dataset'+dataset_id)

    # Get and deserialize
    dataset = request.get_json().get('data', None)
    table_name = dataset.get('attributes').get('tableName')
    scenario, model, _ = table_name.rsplit('/')

    fields = QueryService.get_rasdaman_fields(scenario, model)
    return jsonify(data=fields), 200


@nexgddp_endpoints.route('/rest-datasets/nexgddp', methods=['POST'])
def register_dataset():
    """NEXGDDP REGISTER ENDPOINT"""
    logging.info('Registering new NEXGDDP Dataset')

    # Get and deserialize
    table_name = request.get_json().get('connector').get('table_name')
    try:
        scenario, model, indicator = table_name.rsplit('/')
    except:
        logging.error('Nexgddp tableName Not Valid')
        body = {
            'status': 2,
            'errorMessage': 'Nexgddp tableName Not Valid'
        }
        return jsonify(callback_to_dataset(body)), 200

    # @TODO -> validate dataset
    if True:
        body = {
            'status': 1
        }
    else:
        body = {
            'status': 2,
            'errorMessage': 'Error Validating Nexgddp Dataset'
        }

    return jsonify(callback_to_dataset(body)), 200
