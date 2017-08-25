import json
import jsonpath
import os
from requests import Request, Session
import logging
import tempfile
# from CTRegisterMicroserviceFlask import request_to_microservice
# Need to adapt the CT plugin to allow raw responses


CT_URL = os.getenv('CT_URL')
CT_TOKEN = os.getenv('CT_TOKEN')
API_VERSION = os.getenv('API_VERSION')
def get_raster_file(scenario, model, year, indicator):


    query = 
    
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
