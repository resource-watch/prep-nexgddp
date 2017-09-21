"""QUERY SERVICE"""

import json
import os
import logging
import tempfile
from requests import Request, Session
from nexgddp.errors import SqlFormatError, PeriodNotValid, TableNameNotValid, GeostoreNeeded
from nexgddp.helpers.gdal_helper import GdalHelper
from nexgddp.services.xml_service import XMLService
from CTRegisterMicroserviceFlask import request_to_microservice

from osgeo import gdal, gdalnumeric

RASDAMAN_URL = os.getenv('RASDAMAN_URL')

# Allows gdal to use exceptions
gdal.UseExceptions()

class QueryService(object):
    @staticmethod
    def get_stats(scenario, model, years, indicator, bbox, function):
        logging.info('[QueryService] Getting aggregated temporal series from rasdaman')
        year_min = sorted(years)[0]
        year_max = sorted(years)[-1]
        logging.info(year_min)
        logging.info(year_max)
        results = []
        if bbox == []:
            raise GeostoreNeeded("No latitude and longitude provided")
        else:
            bbox_str = f",Lat({bbox[0]}),Long({bbox[1]})"
            query = f"for cov in ({scenario}_{model}_processed) return encode( {function}((cov.{indicator})[ ansi(\"{year_min}\":\"{year_max}\") {bbox_str}]), \"CSV\")"
            logging.info('Running the query ' + query)
            raster_filename = QueryService.get_rasdaman_query(query)
            try:
                datafile = open(raster_filename, "r")
                raw_data = datafile.read()
                processed_data = raw_data.replace('{', '').replace('}', '').split(',')
            finally:
                source_raster = None
                # Removing the raster
                os.remove(os.path.join('/tmp', raster_filename))
        return map(float, processed_data)
        return results

    @staticmethod
    def get_histogram(scenario, model, years, indicator, bbox):
        logging.info('[QueryService] Getting histogram from rasdaman')
        results = []

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
                all_results = GdalHelper.calc_histogram(source_raster)
                all_results["year"] = year
                results.append(all_results)
            finally:
                source_raster = None
                # Removing the raster
                os.remove(os.path.join('/tmp', raster_filename))
        return results

    @staticmethod
    def get_temporal_series(scenario, model, years, indicator, bbox):
        logging.info('[QueryService] Getting temporal series from rasdaman')
        year_min = sorted(years)[0]
        year_max = sorted(years)[-1]
        logging.info(year_min)
        logging.info(year_max)
        results = []
        if bbox == []:
            raise GeostoreNeeded("No latitude and longitude provided")
        else:
            bbox_str = f",Lat({bbox[0]}),Long({bbox[1]})"
            query = f"for cov in ({scenario}_{model}_processed) return encode( (cov.{indicator})[ ansi(\"{year_min}\":\"{year_max}\") {bbox_str}], \"CSV\")"
            logging.info('Running the query ' + query)
            raster_filename = QueryService.get_rasdaman_query(query)
            try:
                datafile = open(raster_filename, "r")
                raw_data = datafile.read()
                processed_data = raw_data.replace('{', '').replace('}', '').split(',')
            finally:
                source_raster = None
                # Removing the raster
                os.remove(os.path.join('/tmp', raster_filename))
        return map(float, processed_data)


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
            logging.debug(f"[QueryService] Temporary raster filename: {raster_filename}")
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
        output = response.text
        return output

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

    @staticmethod
    def get_domain(scenario, model):
        logging.info(f"Obtaining domain for the scenario {scenario}, and model {model}")
        domain_xml = QueryService.get_rasdaman_fields(scenario, model)
        domain = XMLService.get_domain(domain_xml)
        domain_data = {
            "lat": {
                "max": domain.get('upperCorner')[0],
                "min": domain.get('lowerCorner')[0]
            },
            "lon": {
                "max": domain.get('upperCorner')[1],
                "min": domain.get('lowerCorner')[1]
            },
            "year": {
                "max": domain.get('upperCorner')[2],
                "min": domain.get('lowerCorner')[2]
            }
        }

        logging.debug("Domain data")
        logging.debug(domain_data)

        return domain_data
