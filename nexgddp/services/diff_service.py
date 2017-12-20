import logging
import datetime
import dateutil.parser
from nexgddp.services.query_service import QueryService
from nexgddp.errors import PeriodNotValid
""" Diff service """
class DiffService(object):
    # @staticmethod
    # def execute(config):
    #     response = request_to_microservice(config)
    #     if not response or response.get('errors'):
    #         raise DatasetNotFound(message='Dataset not found')
    #     dataset = response.get('data', None).get('attributes', None)
    #     return dataset
    @staticmethod
    def get_diff_value(dset_a, date_a, date_b, lat, lon, varnames, dset_b):

        dset_b = dset_a if not dset_b else dset_b

        logging.debug(dset_a)
        logging.debug(dset_b)
        results = []
        for var in varnames:
            query = f"for cov1 in ({dset_a}), cov2 in ({dset_b}) return encode((cov1.{var})[ansi(\"{date_a}\"), Lat({lat}),Long({lon})] - (cov2.{var})[ansi(\"{date_b}\"), Lat({lat}),Long({lon})], \"CSV\")]"
            logging.debug(f"query: {query}")
            query_result = QueryService.get_rasdaman_csv_query(query)
            logging.debug(query_result)
            results.append(float(query_result))
        return dict(zip(varnames, results))

    @staticmethod
    def parse_year(value):
        if type(value) is int:
            return value
        else:
            try:
                result = dateutil.parser.parse(value).year
                return int(result)
            except Error as e:
                raise PeriodNotValid("Supplied dates are invalid")

    @staticmethod
    def get_timestep(date_a, date_b):
        years = list(map(lambda x: datetime.datetime(DiffService.parse_year(x), 1, 1).isoformat(), [date_a, date_b]))
        logging.debug(years)
