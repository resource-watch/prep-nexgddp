""" Dataset service """

from RWAPIMicroservicePython import request_to_microservice

from nexgddp.errors import DatasetNotFound


class DatasetService(object):

    @staticmethod
    def execute(uri, api_key):
        response = request_to_microservice(uri=uri, method="GET", api_key=api_key)
        if not response or response.get('errors'):
            raise DatasetNotFound(message='Dataset not found')
        dataset = response.get('data', None).get('attributes', None)
        return dataset

    @staticmethod
    def get(dataset, api_key):
        return DatasetService.execute(uri=f"/v1/dataset/{dataset}", api_key=api_key)
