"""QUERY SERVICE"""

import json
import os
import logging
import tempfile
from requests import Request, Session
from nexgddp.errors import SqlFormatError, PeriodNotValid, TableNameNotValid
from nexgddp.helpers.gdal_helper import GdalHelper
from CTRegisterMicroserviceFlask import request_to_microservice

from osgeo import gdal, gdalnumeric

RASDAMAN_URL = os.getenv('RASDAMAN_URL')

# Allows gdal to use exceptions
gdal.UseExceptions()

class QueryService(object):
    @staticmethod
    def get_stats(scenario, model, years, indicator, bbox, functions):
        logging.info('[QueryService] Getting stats from rasdaman')
        results = {}

        for year in years:
            if bbox == []:
                bbox_str = ""
            else:
                bbox_str = f",Lat({bbox[0]}:{bbox[2]}),Long({bbox[1]}:{bbox[3]})"
            query = f"for cov in ({scenario}_{model}_processed) return encode( (cov.{indicator})[ ansi(\"{year}\") {bbox_str}], \"GTiff\")"
            logging.info('Running the query ' + query)
            raster_filename = QueryService.get_rasdaman_query(query)
            try:
                source_raster = gdal.Open(raster_filename)
                all_results = GdalHelper.calc_stats(source_raster)
                results[year] = {k: all_results[k] for k in functions}
                logging.error("[QueryService] Rasdaman was unable to open the rasterfile")
            finally:
                source_raster = None
                # Removing the raster
                os.remove(os.path.join('/tmp', raster_filename))
            logging.debug("Results")
            logging.debug(results)
        return results


    @staticmethod
    def get_histogram(scenario, model, years, indicator, bbox):
        logging.info('[QueryService] Getting histogram from rasdaman')
        results = {}

        for year in years:
            if bbox == []:
                bbox_str = ""
            else:
                bbox_str = f",Lat({bbox[0]}:{bbox[2]}),Long({bbox[1]}:{bbox[3]})"
            query = f"for cov in ({scenario}_{model}_processed) return encode( (cov.{indicator})[ ansi(\"{year}\") {bbox_str}], \"GTiff\")"
            logging.info('Running the query ' + query)
            raster_filename = QueryService.get_rasdaman_query(query)
            try:
                source_raster = gdal.Open(raster_filename)
                results[year] = GdalHelper.calc_histogram(source_raster)
                logging.error("[QueryService] Rasdaman was unable to open the rasterfile")
            finally:
                source_raster = None
                # Removing the raster
                os.remove(os.path.join('/tmp', raster_filename))
            logging.debug("Results")
            logging.debug(results)
        return results



    @staticmethod
    def get_temporal_series(scenario, model, indicator, lat, lon):
        logging.info('[QueryService] Getting raster from rasdaman')
        query = f"for cov in ({scenario}_{model}_processed) return encode( (cov.{indicator})[Lat({lat}), Long({lon})], \"CSV\")"
        return QueryService.get_rasdaman_query(query)

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
            method='POST',
            url=RASDAMAN_URL,
            data=payload,
            headers=headers
        )
        session = Session()
        prepped = session.prepare_request(request)
        response = session.send(prepped)
        if response.status_code == 404:
            raise PeriodNotValid('Period Not Valid')

        with tempfile.NamedTemporaryFile(suffix='.tiff', delete=False) as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
            raster_filename = f.name
            logging.debug("Raster filename")
            logging.debug(raster_filename)
            f.close()
            return raster_filename

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
            method='GET',
            url=RASDAMAN_URL,
            headers=headers,
            params=params
        )
        session = Session()
        prepped = session.prepare_request(request)
        response = session.send(prepped)
        if response.status_code == 404:
            raise TableNameNotValid('Table Name Not Valid')

        logging.debug(response.url)
        return response.text

    @staticmethod
    def convert(query):
        logging.info('Converting Query: '+query)
        try:
            config = {
                'uri': '/convert/sql2SQL?sql='+query,
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
