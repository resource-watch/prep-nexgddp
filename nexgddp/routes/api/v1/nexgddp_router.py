"""API ROUTER"""

import logging

from flask import jsonify, request, Blueprint, Response
from nexgddp.routes.api import error
from nexgddp.services.query_service import QueryService
from nexgddp.services.xml_service import XMLService
from nexgddp.errors import SqlFormatError, PeriodNotValid, TableNameNotValid, GeostoreNeeded, XMLParserError, InvalidField
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
        raise Exception() # No * allowed
    else:
        def is_function(clause):
            if clause.get('type') == 'function' and clause.get('arguments') and len(clause.get('arguments')) > 0:
                return {
                    'function': clause.get('value'),
                    'argument': clause.get('arguments')[0].get('value')
                }

        def is_literal(clause):
            if clause.get('type') == 'literal':
                return {
                    'function': 'temporal_series',
                    'argument': clause.get('value')
                }
        
        select_functions = list(map(is_function, select_sql))
        select_literals  = list(map(is_literal,  select_sql))

        select = list(filter(None, select_functions + select_literals))
        
        logging.info(select)
    if select == [None] or len(select) == 0 or select is None:
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
    elif where_sql.get('type', None) == 'conditional' or where_sql.get('type', None) == 'operator':
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
        if len(years) > 1:
            years = list(range(years[0], years[1]+1))
        else:
            years = years
        return years


def get_query_type(select):
    query_type = None
    query_indicator = None
    for clause in select:
        query_indicator = clause.get('argument')
        if clause.get('function') == 'st_histogram':
            query_type = 'st_histogram'
            break
        elif clause.get('function') == 'temporal_series':
            query_type = 'temporal_series'
            break
        else:
            pass
    return query_type, query_indicator


@nexgddp_endpoints.route('/query/<dataset_id>', methods=['POST'])
@get_bbox_by_hash
def query(dataset_id, bbox):
    """NEXGDDP QUERY ENDPOINT"""
    logging.info('[ROUTER] Doing Query of dataset '+dataset_id)

    # Get and deserialize
    dataset = request.get_json().get('dataset', None).get('data', None)
    table_name = dataset.get('attributes').get('tableName')
    scenario, model = table_name.rsplit('/')
    sql = request.args.get('sql', None) or request.get_json().get('sql', None)

    if not sql:
        return error(status=400, detail='sql must be provided')

    # convert
    try:
        _, json_sql = QueryService.convert(sql)
        logging.debug("json_sql")
        logging.debug(json_sql)
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

    # Query type
    special_query_type, query_indicator = get_query_type(select)

    # Fields
    fields_xml = QueryService.get_rasdaman_fields(scenario, model)
    fields = XMLService.get_fields(fields_xml)

    try:
        if special_query_type:
            if query_indicator not in fields:
                raise InvalidField(message='Invalid Fields')
            if special_query_type == 'st_histogram':
                response = QueryService.get_histogram(scenario, model, years, query_indicator, bbox)
            elif special_query_type == 'temporal_series':
                response = QueryService.get_temporal_series(scenario, model, years, query_indicator, bbox)
        else:
            for idx in range(0, len(select)):
                if select[idx].get('argument') not in fields:
                    del select[idx]
            if len(select) == 0:
                raise InvalidField(message='Invalid Fields')
            logging.info(select)
            response = QueryService.get_stats(scenario, model, years, bbox, select)
    except InvalidField as e:
        return error(status=400, detail=e.message)
    except PeriodNotValid as e:
        return error(status=400, detail=e.message)
    except GeostoreNeeded as e:
        return error(status=400, detail=e.message)
    return jsonify(data=response), 200


@nexgddp_endpoints.route('/fields/<dataset_id>', methods=['POST'])
def get_fields(dataset_id):
    """NEXGDDP FIELDS ENDPOINT"""
    logging.info('[ROUTER] Getting fields of dataset'+dataset_id)

    # Get and deserialize
    dataset = request.get_json().get('dataset', None).get('data', None)
    table_name = dataset.get('attributes').get('tableName')
    scenario, model = table_name.rsplit('/')

    fields_xml = QueryService.get_rasdaman_fields(scenario, model)
    fields = XMLService.get_fields(fields_xml)
    data = {
        'tableName': table_name,
        'fields': fields
    }
    return jsonify(data), 200


@nexgddp_endpoints.route('/rest-datasets/nexgddp', methods=['POST'])
def register_dataset():
    """NEXGDDP REGISTER ENDPOINT"""
    logging.info('Registering new NEXGDDP Dataset')

    # Get and deserialize
    table_name = request.get_json().get('connector').get('table_name')
    try:
        scenario, model = table_name.rsplit('/')
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
