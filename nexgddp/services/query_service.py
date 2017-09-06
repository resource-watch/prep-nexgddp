"""QUERY SERVICE"""

import json
import os
import logging
import tempfile
from requests import Request, Session
from nexgddp.errors import SqlFormatError
from CTRegisterMicroserviceFlask import request_to_microservice

RASDAMAN_URL = os.getenv('RASDAMAN_URL')

class QueryService(object):

    @staticmethod
    def get_raster_file(scenario, model, year, indicator):
        logging.info('[QueryService] Getting raster from rasdaman')
        query = f"for cov in ({scenario}_{model}_processed) return encode( (cov.{indicator})[ ansi(\"{year}\")], \"PNG\")"
        raster, content_type = QueryService.get_rasdaman_query(query)
        return (raster, content_type)

    @staticmethod
    def get_temporal_series(scenario, model, indicator, lat, lon):
        logging.info('[QueryService] Getting raster from rasdaman')
        query = f"for cov in ({scenario}_{model}_processed) return encode( (cov.{indicator})[Lat({lat}), Long({lon})], \"CSV\")"
        temporal_series, content_type = QueryService.get_rasdaman_query(query)
        return (temporal_series, content_type)

    @staticmethod
    def get_rasdaman_query(query):
        logging.info('[QueryService] Executing rasdaman query')
        payload_arr = ['<?xml version="1.0" encoding="UTF-8" ?>',
	           '<ProcessCoveragesRequest xmlns="http://www.opengis.net/wcps/1.0" service="WCPS" version="1.0.0">',
	           '<query><abstractSyntax>',
	           query,
	           '</abstractSyntax></query>',
	           '</ProcessCoveragesRequest>']
        payload = ''.join(payload_arr)
        headers = {'Content-Type': 'application/xml'}
        request = Request(
            method = 'POST',
            url = RASDAMAN_URL,
            data = payload,
            headers = headers
        )
        session = Session()
        prepped = session.prepare_request(request)
        response = session.send(prepped)

        def generate_response():
            for chunk in response.iter_content(chunk_size=1024):
                yield(chunk)
        return (generate_response(), response.headers['content-type'])

    @staticmethod
    def get_rasdaman_fields(scenario, model):
        # Need to parse xml
        logging.info(f"[QueryService] Getting fields for scenario {scenario} and model {model}")
        headers = {'Content-Type': 'application/xml'}
        params = {
            'SERVICE': 'WCS',
            'VERSION': '2.0.1',
            'REQUEST': 'DescribeCoverage', 
            'COVERAGEID': f"{scenario}_{model}_processed"
        }
        request = Request(
            method = 'GET',
            url = RASDAMAN_URL,
            headers = headers,
            params = params
        )
        session = Session()
        prepped = session.prepare_request(request)
        response = session.send(prepped)
        logging.debug(response.url)
        return response.text

    
    @staticmethod
    def convert(query):
        logging.info('Converting Query: '+query)
        try:
            config = {
                'uri': '/convert/sql2SQL',
                'method': 'GET'
            }
            response = request_to_microservice(config)
        except Exception as error:
            raise error

        if response.get('errors'):
            errors = response.get('errors')
            raise SqlFormatError(message=errors[0].get('detail'))

        query = response.get('data', None)
        s_query = query.get('attributes', {}).get('query')
        json_sql = query.get('attributes', {}).get('jsonSql')
        return s_query, json_sql

    @staticmethod
    def get_clone_url(dataset_id, query):
        return {
            'httpMethod': 'POST',
            'url': '/v1/dataset/'+dataset_id+'/clone',
            'body': {
                'dataset': {
                    'datasetUrl': '/query/'+dataset_id+'?sql='+query,
                    'application': [
                        'your',
                        'apps'
                    ]
                }
            }
        }
