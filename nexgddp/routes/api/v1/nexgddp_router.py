"""API ROUTER"""
import logging
import os
from flask import jsonify, request, send_from_directory, Blueprint, Response
from nexgddp.routes.api import error
from nexgddp.services.query_service import QueryService
from nexgddp.services.xml_service import XMLService
from nexgddp.services.tile_service import TileService
from nexgddp.services.storage_service import StorageService
from nexgddp.services.redis_service import RedisService
from nexgddp.helpers.coloring_helper import ColoringHelper
from nexgddp.errors import SqlFormatError, PeriodNotValid, TableNameNotValid, GeostoreNeeded, XMLParserError, InvalidField, CoordinatesNeeded, LayerNotFound
from nexgddp.middleware import get_bbox_by_hash, get_latlon, get_tile_attrs, get_layer, tile_exists
from CTRegisterMicroserviceFlask import request_to_microservice
import datetime
import dateutil.parser

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
    logging.debug(f"select_sql: {select_sql}")
    select_sql
    select = None
    if len(select_sql) == 1 and select_sql[0].get('value') == '*':
        select = [{'function': 'temporal_series', 'argument': 'all'}]
        return select
        # raise Exception() # No * allowed
    else:
        def is_function(clause):
            if clause.get('type') == 'function' and clause.get('arguments') and len(clause.get('arguments')) > 0:
                return {
                    'function': clause.get('value').lower(),
                    'argument': clause.get('arguments')[0].get('value'),
                    'alias': clause.get('alias', None)
                }

        def is_literal(clause):
            if clause.get('type') == 'literal':
                return {
                    'function': 'temporal_series',
                    'argument': clause.get('value'),
                    'alias': clause.get('alias', None)
                }
        
        select_functions = list(map(is_function, select_sql))
        select_literals  = list(map(is_literal,  select_sql))
        select = list(filter(None, select_functions + select_literals))

    # Reductions and temporal series have different cardinality - can't be both at the same time
    if not all(val is None for val in select_functions) and not all(val is None for val in select_literals):
        logging.error("Provided functions and literals at the same time")
        raise Exception()
    # And it's neccesary to select something
    if select == [None] or len(select) == 0 or select is None:
        raise Exception()
    return select

def parse_year(value):
    if type(value) is int:
        return value
    else:
        try:
            result = dateutil.parser.parse(value).year
            return int(result)
        except Error as e:
            raise PeriodNotValid("Supplied dates are invalid")
            

def get_years(json_sql):
    where_sql = json_sql.get('where', None)
    logging.debug(f"where_sql: {where_sql}")
    if where_sql is None:
        return []
    years = []
    if where_sql.get('type', None) == 'between':
        years = list(
            map(
                lambda argument: parse_year(argument.get('value')),
                where_sql.get('arguments')
            )
        )
        logging.debug(years)
        years = list(range(parse_year(years[0]), parse_year(years[1])+1))
        logging.debug(years)
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
            years = list(range(
                parse_year(years[0]),
                parse_year(years[1])+1 )
            )
        else:
            years = years
        return years

@nexgddp_endpoints.route('/query/<dataset_id>', methods=['POST'])
@get_bbox_by_hash
@get_latlon
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
    except SqlFormatError as e:
        logging.error(e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: '+str(e))
        return error(status=500, detail='Generic Error')
    try:
        select = get_sql_select(json_sql)
        logging.debug("Select")
        logging.debug(select)
    except Exception as e:
        return error(status=400, detail='Invalid Select')


    # Fields
    fields_xml = QueryService.get_rasdaman_fields(scenario, model)
    fields = XMLService.get_fields(fields_xml)
    fields.update({'all': {'type': 'array'}})
    
    # Prior to validating dates, the [max|min](year) case has to be dealt with:
    def is_year(clause):
        if (clause.get('function') == 'max' or  clause.get('function') == 'min') and clause.get('argument') == 'year':
            return True
        else:
            return False
    # All statements in the select must have the prior form
    select_year = all(list(map(is_year, select)))

    if select_year == True:
        result = {}
        domain = QueryService.get_domain(scenario, model)
        for element in select:
            result[element['alias'] if element['alias'] else f"{element['function']}({element['argument']})"] = domain.get(element['argument']).get(element['function'])
        return jsonify(data=[result]), 200
    
    if not bbox:
        return error(status=400, detail='No coordinates provided. Include geostore or lat & lon')
    # Get years
    years = get_years(json_sql)
    if len(years) == 0:
        domain = QueryService.get_domain(scenario, model)
        logging.debug(f"domain: {domain}")
        years = list(range(
            int(dateutil.parser.parse(domain['year']['min'], fuzzy_with_tokens=True)[0].year),
            int(dateutil.parser.parse(domain['year']['max'], fuzzy_with_tokens=True)[0].year + 1)
        ))
        # return error(status=400, detail='Period of time must be set')

    results = {}
    for element in select:
        try:
            if element['argument'] not in fields:
                raise InvalidField(message='Invalid Fields')
            elif element['function'] == 'temporal_series' and element['argument'] == 'year':
                results[element['alias'] if element['alias'] else 'year'] = map(lambda x: datetime.datetime(parse_year(x), 1, 1).isoformat(), years)
            elif element['function'] == 'temporal_series' and element['argument'] == 'all':
                query_results = QueryService.get_all_data(scenario, model, years, bbox)
                return jsonify(data = query_results), 200
            elif element['function'] == 'temporal_series':
                indicator = element['argument']
                results[element['alias'] if element['alias'] else indicator] = QueryService.get_temporal_series(scenario, model, years, indicator, bbox)
            else:
                function = element['function']
                indicator = element['argument']
                results[element['alias'] if element['alias'] else f"{function}({indicator})"] = QueryService.get_stats(scenario, model, years, indicator, bbox, function)
        except InvalidField as e:
            return error(status=400, detail=e.message)
        except PeriodNotValid as e:
            return error(status=400, detail=e.message)
        except GeostoreNeeded as e:
            return error(status=400, detail=e.message)
        except CoordinatesNeeded as e:
            return error(status=400, detail=e.message) 
    output = [dict(zip(results, col)) for col in zip(*results.values())]
    # return jsonify(data=response), 200
    return jsonify(data=output), 200


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


@nexgddp_endpoints.route('/layer/<layer>/tile/nexgddp/<int:z>/<int:x>/<int:y>', methods=['GET'])
@tile_exists
@get_layer
@get_tile_attrs
def get_tile(x, y, z, model, scenario, year, style, indicator, layer):
    """Slippy map endpoint"""
    logging.info(f'Getting tile for {x} {y} {z}')
    bbox = TileService.get_bbox(z, x, y)
    logging.debug(f"bbox: {bbox}")
    rasterfile = QueryService.get_tile_query(bbox, year, model, scenario, indicator)
    colored_response = ColoringHelper.colorize(rasterfile, color_ramp_name = style)

    # Saving file in cache
    logging.debug(f'Requested path is: {request.path}')

    # Uploading file to storage
    # Beware of side effects!
    # ColoringHelper.colorize stores the color-coded file in the same input file
    # Uploading file to storage. 
    StorageService.upload_file(rasterfile, layer, str(z), str(x), str(y))
    
    return colored_response, 200
