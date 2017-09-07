"""API ROUTER"""

import logging

from flask import jsonify, request, Blueprint, Response
from nexgddp.routes.api import error
from nexgddp.services.query_service import QueryService
from nexgddp.errors import SqlFormatError, PeriodNotValid, TableNameNotValid
from nexgddp.middleware import get_bbox_by_hash
from CTRegisterMicroserviceFlask import request_to_microservice

nexgddp_endpoints = Blueprint('nexgddp_endpoints', __name__)


def callback_to_dataset(body):
    config = {
        'uri': '/dataset/'+request.get_json().get('connector').get('id'),
        'method': 'PATCH',
        'body': body
    }
    return request_to_microservice(config)

def get_sql_select(json_sql):
    select_sql = json_sql.get('select')
    select = None
    if len(select_sql) == 1 and select_sql[0].get('value') == '*':
        select = ['min', 'max', 'avg', 'stdev']  # @TODO
    else:
        def is_function(clause):
            if clause.get('type') == 'function':
                return clause.get('value')

        select = list(map(is_function, select_sql))

    if select == [None]:
        raise Exception()

    return select


def get_years(json_sql):
    where_sql = json_sql.get('where', None)
    if where_sql is None:
        return []
    years = []
    if where_sql.get('type', None) == 'between':
        years = list(map(lambda argument: argument.get('value'), where_sql.get('arguments')))
        years = list(range(years[0], years[1]+1))
        return years
    elif where_sql.get('type', None) == 'conditional':
        def get_years_node(node):
            if node.get('type') == 'operator':
                if node.get('left').get('value') == 'year' or node.get('left').get('value') == 'year':
                    value = node.get('left') if node.get('left').get('type') == 'number' else node.get('right')
                    years.append(value.get('value'))
            else:
                if node.get('type') == 'conditional':
                    get_years_node(node.get('left'))
                    get_years_node(node.get('right'))

        get_years_node(where_sql)
        years.sort()
        years = list(range(years[0], years[1]+1))
        return years


@nexgddp_endpoints.route('/query/<dataset_id>', methods=['POST'])
@get_bbox_by_hash
def query(dataset_id, bbox):
    """NEXGDDP QUERY ENDPOINT"""
    logging.info('[ROUTER] Doing Query of dataset '+dataset_id)

    # Get and deserialize
    dataset = request.get_json().get('dataset', None).get('data', None)
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

    # Get select
    try:
        select = get_sql_select(json_sql)
    except Exception as e:
        return error(status=400, detail='Invalid Select')

    # Get years
    years = get_years(json_sql)
    if len(years) == 0:
        return error(status=400, detail='Period of time must be set')

    logging.debug(select)
    logging.debug(years)
    logging.debug(bbox)

    try:
        if 'st_histogram' in select:
            response = QueryService.get_histogram(scenario, model, years, indicator, bbox)
        else:
            response = QueryService.get_stats(scenario, model, years, indicator, bbox, select)
    except PeriodNotValid as e:
        return error(status=500, detail=e.message)
    return jsonify(data=[response]), 200


@nexgddp_endpoints.route('/fields/<dataset_id>', methods=['POST'])
def get_fields(dataset_id):
    """NEXGDDP FIELDS ENDPOINT"""
    logging.info('[ROUTER] Getting fields of dataset'+dataset_id)

    # Get and deserialize
    dataset = request.get_json().get('dataset', None).get('data', None)
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
        scenario, model, _ = table_name.rsplit('/')
    except Exception:
        logging.error('Nexgddp tableName Not Valid')
        body = {
            'status': 2,
            'errorMessage': 'Nexgddp tableName Not Valid'
        }
        return jsonify(callback_to_dataset(body)), 200

    try:
        QueryService.get_rasdaman_fields(scenario, model)
    except TableNameNotValid:
        body = {
            'status': 2,
            'errorMessage': 'Error Validating Nexgddp Dataset'
        }
        return jsonify(callback_to_dataset(body)), 200

    body = {
        'status': 1
    }

    return jsonify(callback_to_dataset(body)), 200
