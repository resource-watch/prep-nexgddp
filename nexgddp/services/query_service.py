"""QUERY SERVICE"""

import json
import os
import logging
import tempfile
from requests import Request, Session
from nexgddp.errors import SqlFormatError
from CTRegisterMicroserviceFlask import request_to_microservice
# Need to adapt the CT plugin to allow raw responses

CT_URL = os.getenv('CT_URL')
CT_TOKEN = os.getenv('CT_TOKEN')
API_VERSION = os.getenv('API_VERSION')


class QueryService(object):

    @staticmethod
    def get_raster_file(scenario, model, year, indicator):
        # query =

        logging.info('[QueryService] Getting raster from rasdaman')
        request_url = CT_URL + '/' + API_VERSION + '/query/' + dataset
        session = Session()
        request = Request(
            method = "POST",
            url = request_url,
            headers = {
                'content-type': 'application/json',
                'Authorization': 'Bearer ' + CT_TOKEN
            },
            data = json.dumps({"wcps": query})
        )
        prepped = session.prepare_request(request)
        response = session.send(prepped)
        with tempfile.NamedTemporaryFile(suffix='.tiff', delete=False) as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
            raster_filename = f.name
            f.close()
            return raster_filename

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
