"""API ROUTER"""
import io
import logging
import os

import dateutil.parser
from RWAPIMicroservicePython import request_to_microservice
from flask import Flask, jsonify, request, Blueprint, send_file

from nexgddp.errors import SqlFormatError, PeriodNotValid, TableNameNotValid, GeostoreNeeded, InvalidField, \
    CoordinatesNeeded, CoverageNotFound
from nexgddp.helpers.coloring_helper import ColoringHelper
from nexgddp.middleware import get_bbox_by_hash, get_latlon, get_tile_attrs, get_layer, get_year, \
    is_microservice, get_diff_attrs, is_microservice_or_admin, get_dataset_from_id
from nexgddp.routes.api import error
from nexgddp.services.diff_service import DiffService
from nexgddp.services.query_service import QueryService
from nexgddp.services.redis_service import RedisService
from nexgddp.services.storage_service import StorageService
from nexgddp.services.tile_service import TileService
from nexgddp.services.xml_service import XMLService

nexgddp_endpoints = Blueprint('nexgddp_endpoints', __name__)

app = Flask(__name__)


def callback_to_dataset(body, api_key):
    return request_to_microservice(
        uri=f'/v1/dataset/{request.get_json().get("connector").get("id")}',
        method='PATCH',
        body=body,
        api_key=api_key
    )


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
        select_literals = list(map(is_literal, select_sql))
        select = list(filter(None, select_functions + select_literals))
    # Reductions and temporal series have different cardinality - can't be both at the same time
    if not all(val is None for val in select_functions) and not all(val is None for val in select_literals):
        logging.error("Provided functions and literals at the same time")
        raise Exception()
    # And it's necessary to select something
    if select == [None] or len(select) == 0 or select is None:
        raise Exception()
    return select


def parse_year(value):
    if type(value) is int:
        return value
    else:
        try:
            result = dateutil.parser.parse(value).strftime('%Y-%m-%d')
            return result
        except Exception as e:
            raise PeriodNotValid("Supplied dates are invalid")


def get_list_years(resolution):
    return {
        'decadal': [
            '1971-01-01T00:00:00.000Z',
            '1981-01-01T00:00:00.000Z',
            '1991-01-01T00:00:00.000Z',
            '2001-01-01T00:00:00.000Z',
            '2011-01-01T00:00:00.000Z',
            '2021-01-01T00:00:00.000Z',
            '2031-01-01T00:00:00.000Z',
            '2041-01-01T00:00:00.000Z',
            '2051-01-01T00:00:00.000Z',
            '2061-01-01T00:00:00.000Z',
            '2071-01-01T00:00:00.000Z',
            '2081-01-01T00:00:00.000Z'
        ],
        'y': [  # 30_y - to fix
            '1971-01-01T00:00:00.000Z',
            '2021-01-01T00:00:00.000Z',
            '2051-01-01T00:00:00.000Z'
        ],
        'monthly': [
            '1971-01-01T00:00:00.000Z',
            '1971-02-01T00:00:00.000Z',
            '1971-03-01T00:00:00.000Z',
            '1971-04-01T00:00:00.000Z',
            '1971-05-01T00:00:00.000Z',
            '1971-06-01T00:00:00.000Z',
            '1971-07-01T00:00:00.000Z',
            '1971-08-01T00:00:00.000Z',
            '1971-09-01T00:00:00.000Z',
            '1971-10-01T00:00:00.000Z',
            '1971-11-01T00:00:00.000Z',
            '1971-12-01T00:00:00.000Z',
            '2021-01-01T00:00:00.000Z',
            '2021-02-01T00:00:00.000Z',
            '2021-03-01T00:00:00.000Z',
            '2021-04-01T00:00:00.000Z',
            '2021-05-01T00:00:00.000Z',
            '2021-06-01T00:00:00.000Z',
            '2021-07-01T00:00:00.000Z',
            '2021-08-01T00:00:00.000Z',
            '2021-09-01T00:00:00.000Z',
            '2021-10-01T00:00:00.000Z',
            '2021-11-01T00:00:00.000Z',
            '2021-12-01T00:00:00.000Z',
            '2051-01-01T00:00:00.000Z',
            '2051-02-01T00:00:00.000Z',
            '2051-03-01T00:00:00.000Z',
            '2051-04-01T00:00:00.000Z',
            '2051-05-01T00:00:00.000Z',
            '2051-06-01T00:00:00.000Z',
            '2051-07-01T00:00:00.000Z',
            '2051-08-01T00:00:00.000Z',
            '2051-09-01T00:00:00.000Z',
            '2051-10-01T00:00:00.000Z',
            '2051-11-01T00:00:00.000Z',
            '2051-12-01T00:00:00.000Z'
        ],
        'seasonal': []
    }.get(resolution, [])


def get_years_where(where_sql, temporal_resolution):
    if where_sql["type"] == 'between':
        start_stop_years = sorted(list(map(lambda arg: arg['value'], where_sql['arguments'])))
        logging.debug(f"start_stop_years: {start_stop_years}")
        parsed = sorted(list(map(
            lambda date: dateutil.parser.parse(str(date), fuzzy=True, ignoretz=True, dayfirst=False,
                                               yearfirst=True).replace(tzinfo=None), start_stop_years)))
        logging.debug(parsed[0].isoformat())
        all_years = list(map(lambda date: dateutil.parser.parse(date, fuzzy=True).replace(tzinfo=None),
                             get_list_years(temporal_resolution)))
        final_years = list(map(
            lambda date: date.isoformat(),
            [date for date in all_years if date <= parsed[1] and date >= parsed[0]]
        ))
        if final_years == []:
            raise PeriodNotValid("Supplied dates are invalid")
    elif where_sql["type"] == "operator" and where_sql["value"] == '=':
        current_year = where_sql['right']['value'].strip("'")
        final_years = [current_year, current_year]
    return final_years


def get_years(json_sql, temporal_resolution):
    list_years = get_list_years(temporal_resolution)
    logging.debug(f"temporal_resolution: {temporal_resolution}")
    logging.debug(f"list_years: {list_years}")
    where_sql = json_sql.get('where', None)
    logging.debug(f"where_sql: {where_sql}")
    if where_sql is None:
        return list_years
    query_dates = get_years_where(where_sql, temporal_resolution)
    logging.debug(f"query_dates: {query_dates}")
    return query_dates


@nexgddp_endpoints.route('/query/<dataset_id>', methods=['GET', 'POST'])
@get_dataset_from_id
@get_bbox_by_hash
@get_latlon
def query(dataset_id, bbox, dataset):
    """NEXGDDP QUERY ENDPOINT"""
    logging.info('[ROUTER] Doing Query of dataset ' + dataset_id)
    # Get and deserialize
    table_name = dataset.get('tableName')
    temporal_resolution = table_name.split('_')[-1]
    logging.debug(f"temporal_resolution: {temporal_resolution}")
    scenario, model = table_name.rsplit('/')

    if request.method == "POST":
        sql = request.get_json().get("sql", None)
    else:
        sql = request.args.get('sql', None)

    if not sql:
        return error(status=400, detail='sql must be provided')
    # convert
    try:
        _, json_sql = QueryService.convert(sql, api_key=request.headers.get('x-api-key'))
    except SqlFormatError as e:
        logging.error(e.message)
        return error(status=500, detail=e.message)
    except Exception as e:
        logging.error('[ROUTER]: ' + str(e))
        return error(status=500, detail='Generic Error')
    try:
        select = get_sql_select(json_sql)
        logging.debug("Select")
        logging.debug(select)
    except Exception as e:
        return error(status=400, detail='Invalid Select')
    # Fields
    try:
        fields_xml = QueryService.get_rasdaman_fields(scenario, model)
        fields = XMLService.get_fields(fields_xml)
    except TableNameNotValid as e:
        return error(status=404, detail='Table name not valid')
    fields.update({'all': {'type': 'array'}})

    # Prior to validating dates, the [max|min](year) case has to be dealt with:
    def is_year(clause):
        if (clause.get('function') == 'max' or clause.get('function') == 'min') and clause.get('argument') == 'year':
            return True
        else:
            return False

    # All statements in the select must have the prior form
    select_year = all(list(map(is_year, select)))
    if select_year == True:
        result = {}
        domain = QueryService.get_domain(scenario, model)
        logging.debug(f"domain: {domain}")
        for element in select:
            result[
                element['alias'] if element['alias'] else f"{element['function']}({element['argument']})"] = domain.get(
                element['argument']).get(element['function'])
        return jsonify(data=[result]), 200
    if not bbox:
        return error(status=400, detail='No coordinates provided. Include geostore or lat & lon')
    # Get years
    try:
        years = get_years(json_sql, temporal_resolution)
    except PeriodNotValid as e:
        return error(status=400, detail=e.message)
    logging.debug("years: ")
    logging.debug(years)
    # if len(years) == 0:
    #     domain = QueryService.get_domain(scenario, model)
    #     logging.debug(f"domain: {domain}")
    #     years = list(range(
    #         int(dateutil.parser.parse(domain['year']['min'], fuzzy_with_tokens=True)[0].year),
    #         int(dateutil.parser.parse(domain['year']['max'], fuzzy_with_tokens=True)[0].year + 1),
    #         10 
    #     )) if temporal_resolution == 'decadal' else ['1971', '2021', '2051']
    #     logging.debug(f"years: {years}")
    # return error(status=400, detail='Period of time must be set')

    results = {}
    for element in select:
        try:
            if element['argument'] not in fields:
                raise InvalidField(message='Invalid Fields')
            elif element['function'] == 'temporal_series' and element['argument'] == 'year':
                results[element['alias'] if element['alias'] else 'year'] = years
            elif element['function'] == 'temporal_series' and element['argument'] == 'all':
                query_results = QueryService.get_all_data(scenario, model, years, bbox)
                return jsonify(data=query_results), 200
            elif element['function'] == 'temporal_series':
                indicator = element['argument']
                results[element['alias'] if element['alias'] else indicator] = QueryService.get_temporal_series(
                    scenario, model, years, indicator, bbox)
            else:
                function = element['function']
                indicator = element['argument']
                results[element['alias'] if element['alias'] else f"{function}({indicator})"] = QueryService.get_stats(
                    scenario, model, years, indicator, bbox, function)
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


@nexgddp_endpoints.route('/fields/<dataset_id>', methods=['GET', 'POST'])
@get_dataset_from_id
def get_fields(dataset_id, dataset):
    """NEXGDDP FIELDS ENDPOINT"""
    logging.info('[ROUTER] Getting fields of dataset' + dataset_id)

    # Get and deserialize
    table_name = dataset.get('tableName')
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
        return jsonify(callback_to_dataset(body, request.headers.get('x-api-key'))), 200

    try:
        QueryService.get_rasdaman_fields(scenario, model)
    except TableNameNotValid:
        body = {
            'status': 2,
            'errorMessage': 'Error Validating Nexgddp Dataset'
        }
        return jsonify(callback_to_dataset(body, request.headers.get('x-api-key'))), 200

    body = {
        'status': 1
    }

    return jsonify(callback_to_dataset(body, request.headers.get('x-api-key'))), 200


@nexgddp_endpoints.route('/layer/<layer>/tile/nexgddp/<int:z>/<int:x>/<int:y>', methods=['GET'])
@get_layer
@get_year
@get_tile_attrs
def get_tile(z, x, y, model, scenario, year, style, indicator, layer, compare_year=None, dset_b=None, no_data=None):
    logging.info(f'Getting tile for {z} {x} {y}')
    logging.debug(compare_year)
    bbox = TileService.get_bbox(x, y, z)
    logging.debug(f"bbox: {bbox}")
    bounds = ColoringHelper.get_data_bounds(style)
    logging.debug(bounds)
    if compare_year:
        logging.debug(f"[rout] compare_year: {compare_year}")
        if not dset_b:
            dset_b = f"{scenario}_{model}_processed"
        rasterfile = QueryService.get_tile_diff_query(bbox, year, model, scenario, indicator, bounds, compare_year,
                                                      dset_b)
    else:
        rasterfile = QueryService.get_tile_query(bbox, year, model, scenario, indicator, bounds)

    try:
        colored_response = ColoringHelper.colorize(rasterfile, style=style)
    except CoverageNotFound as e:
        return error(status=404, detail=e.message)

    logging.debug(f"colored_response: {colored_response}")
    # logging.debug(f"colored_response.shape: {colored_response.shape}")

    if no_data is not None:
        logging.debug("Creating mask")
        maskfile = QueryService.get_tile_mask_query(bbox, year, model, scenario, indicator, no_data)
        ColoringHelper.blend_alpha(colored_response, maskfile)
        os.remove(maskfile)
    else:
        logging.debug("No nodata values")

    # Saving file in cache
    logging.debug(f'Requested path is: {request.path}')

    # Uploading file to storage. 
    StorageService.upload_file(colored_response, layer, str(z), str(x), str(y), year, compare_year, dset_b)

    tile_response = send_file(
        io.BytesIO(open(colored_response, 'rb').read()),
        attachment_filename='tile.png',
        mimetype='image/png'
    )

    os.remove(colored_response)
    return tile_response, 200
    # return mask_response, 200


@nexgddp_endpoints.route('/layer/nexgddp/<layer>/expire-cache', methods=['DELETE'])
@nexgddp_endpoints.route('/layer/loca/<layer>/expire-cache', methods=['DELETE'])
@is_microservice
def expire_cache(layer):
    logging.info('[NEXGDDP-ROUTER] Expiring the tile cache for nexgddp/loca layer')
    RedisService.expire_layer(layer)
    StorageService.delete_folder(layer)
    return jsonify({"result": 'OK'}), 200


@nexgddp_endpoints.route('/diff', methods=['POST'])
@get_diff_attrs
def diff(dset_a, date_a, date_b, lat, lon, varnames, dset_b=None):
    logging.debug('[NEXGDDP-ROUTER] Calculating diff')
    diff_value = DiffService.get_diff_value(dset_a, date_a, date_b, lat, lon, varnames, dset_b)
    # diff_timestep = DiffService.get_timestep(date_a, date_b)
    return jsonify({"value": diff_value}), 200


@nexgddp_endpoints.route('/info/<indicator>', methods=['GET'])
def getInfoIndicator(indicator):
    logging.info('[NEXGDDP-ROUTER] Get info of indicator')
    body = {}
    body["scenarios"] = [{
        "label": "Low Emissions",
        "id": "rcp45"
    }, {
        "label": "High Emissions",
        "id": "rcp85"
    }]
    body["temporalResolution"] = [{
        "label": "Decadal",
        "id": "decadal",
        "periods": [{
            "label": "1971-1980",
            "id": "1971-01-01T00:00:00.000Z"
        }, {
            "label": "1981-1990",
            "id": "1981-01-01T00:00:00.000Z"
        }, {
            "label": "1991-2000",
            "id": "1991-01-01T00:00:00.000Z"
        }, {
            "label": "2001-2010",
            "id": "2001-01-01T00:00:00.000Z"
        }, {
            "label": "2011-2020",
            "id": "2011-01-01T00:00:00.000Z"
        }, {
            "label": "2021-2030",
            "id": "2021-01-01T00:00:00.000Z"
        }, {
            "label": "2031-2040",
            "id": "2031-01-01T00:00:00.000Z"
        }, {
            "label": "2041-2050",
            "id": "2041-01-01T00:00:00.000Z"
        }, {
            "label": "2051-2060",
            "id": "2051-01-01T00:00:00.000Z"
        }, {
            "label": "2061-2070",
            "id": "2061-01-01T00:00:00.000Z"
        }, {
            "label": "2071-2080",
            "id": "2071-01-01T00:00:00.000Z"
        }, {
            "label": "2081-2090",
            "id": "2081-01-01T00:00:00.000Z"
        }]
    }, {
        "label": "30 years",
        "id": "30_y",
        "periods": [{
            "label": "1971-2000",
            "id": "1971-01-01T00:00:00.000Z"
        }, {
            "label": "2021-2050",
            "id": "2021-01-01T00:00:00.000Z"
        }, {
            "label": "2051-2080",
            "id": "2051-01-01T00:00:00.000Z"
        }]
    }]
    return jsonify(body), 200


@nexgddp_endpoints.route('/locainfo/<indicator>', methods=['GET'])
def getLocaInfoIndicator(indicator):
    logging.info('[NEXGDDP-ROUTER] Get info of indicator (LOCA)')
    body = {}
    body["scenarios"] = [{
        "label": "Low Emissions",
        "id": "rcp45"
    }, {
        "label": "High Emissions",
        "id": "rcp85"
    }]
    body["temporalResolution"] = [{
        "label": "Decadal",
        "id": "decadal",
        "periods": [{
            "label": "1971-1980",
            "id": "1971-01-01T00:00:00.000Z"
        }, {
            "label": "1981-1990",
            "id": "1981-01-01T00:00:00.000Z"
        }, {
            "label": "1991-2000",
            "id": "1991-01-01T00:00:00.000Z"
        }, {
            "label": "2001-2010",
            "id": "2001-01-01T00:00:00.000Z"
        }, {
            "label": "2011-2020",
            "id": "2011-01-01T00:00:00.000Z"
        }, {
            "label": "2021-2030",
            "id": "2021-01-01T00:00:00.000Z"
        }, {
            "label": "2031-2040",
            "id": "2031-01-01T00:00:00.000Z"
        }, {
            "label": "2041-2050",
            "id": "2041-01-01T00:00:00.000Z"
        }, {
            "label": "2051-2060",
            "id": "2051-01-01T00:00:00.000Z"
        }, {
            "label": "2061-2070",
            "id": "2061-01-01T00:00:00.000Z"
        }, {
            "label": "2071-2080",
            "id": "2071-01-01T00:00:00.000Z"
        }, {
            "label": "2081-2090",
            "id": "2081-01-01T00:00:00.000Z"
        }]
    }, {
        "label": "30 years",
        "id": "30_y",
        "periods": [{
            "label": "1971-2000",
            "id": "1971-01-01T00:00:00.000Z"
        }, {
            "label": "2021-2050",
            "id": "2021-01-01T00:00:00.000Z"
        }, {
            "label": "2051-2080",
            "id": "2051-01-01T00:00:00.000Z"
        }]
    }]
    return jsonify(body), 200


@nexgddp_endpoints.route('/dataset/<indicator>/<scenario>/<temporal_res>', methods=['GET'])
def getDataset(indicator, scenario, temporal_res):
    logging.info('[NEXGDDP-ROUTER] Get info of indicator')
    datasets = request_to_microservice(
        uri=f'/v1/dataset?includes=layer&tableName={indicator}/{scenario}_{temporal_res}&env={request.args.get("env", "production")}',
        method='GET',
        api_key=request.headers.get('x-api-key')
    )
    if datasets.get('data') and len(datasets.get('data')) > 0:
        return jsonify({"data": datasets.get('data')[0]}), 200
    return jsonify({"errors": [{
        "status": 404,
        "detail": "Dataset doesn't exist"
    }]}), 404


@nexgddp_endpoints.route('/locadataset/<indicator>/<scenario>/<temporal_res>', methods=['GET'])
def getLocaDataset(indicator, scenario, temporal_res):
    logging.info('[NEXGDDP-ROUTER] Get info of indicator')
    datasets = request_to_microservice(
        uri=f'/v1/dataset?includes=layer&tableName=loca_{indicator}/{scenario}_{temporal_res}&env={request.args.get("env", "production")}',
        method='GET',
        api_key=request.headers.get('x-api-key')
    )
    if datasets.get('data') and len(datasets.get('data')) > 0:
        return jsonify({"data": datasets.get('data')[0]}), 200
    return jsonify({"errors": [{
        "status": 404,
        "detail": "Dataset doesn't exist"
    }]}), 404


@nexgddp_endpoints.route('/rest-datasets/nexgddp/<dataset_id>', methods=['DELETE'])
@is_microservice_or_admin
def delete(dataset_id):
    """Delete GEE Dataset"""
    logging.info('Deleting GEE dataset {}'.format(dataset_id))
    return b'', 204
