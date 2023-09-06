"""Geostore SERVICE"""

from RWAPIMicroservicePython import request_to_microservice

from nexgddp.errors import GeostoreNotFound


class GeostoreService(object):
    """."""

    @staticmethod
    def execute(uri, api_key):
        try:
            response = request_to_microservice(uri=uri, method="GET", api_key=api_key)
            if not response or response.get('errors'):
                raise GeostoreNotFound
            geostore = response.get('data', None).get('attributes', None)
            geojson = geostore.get('geojson', None)
            bbox = geostore.get('bbox', None)
        except Exception as e:
            raise GeostoreNotFound(message=str(e))
        return geojson, bbox

    @staticmethod
    def get(geostore, api_key):
        return GeostoreService.execute(uri=f"/v2/geostore/{geostore}", api_key=api_key)
