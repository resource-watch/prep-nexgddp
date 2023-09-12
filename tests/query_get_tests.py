import os
from pathlib import Path

import requests_mock
from RWAPIMicroservicePython.test_utils import mock_request_validation

from tests.mocks import mock_get_dataset
from tests.fixtures import (
    dataset_loca_json,
    dataset_nexgddp_json,
    dataset_invalid_connector_type,
    dataset_invalid_provider
)


@requests_mock.Mocker(kw='mocker')
def test_query_dataset_that_does_not_exist(client, mocker):
    mock_get_dataset(
        mocker,
        status_code=404,
        response_json={"errors": [{"status": 404, "detail": "Dataset with id 'bar' doesn't exist"}]}
    )
    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))

    response = client.get(
        '/api/v1/nexgddp/query/bar', headers={'x-api-key': 'api-key-test'}
    )
    assert response.status_code == 404
    assert response.data == b'{"errors":[{"detail":"Dataset with id bar doesn\'t exist","status":404}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_query_dataset_invalid_provider(client, mocker):
    mock_get_dataset(mocker, dataset_invalid_provider)
    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))

    response = client.get(
        '/api/v1/nexgddp/query/bar', headers={'x-api-key': 'api-key-test'}
    )
    assert response.status_code == 422
    assert response.data == b'{"errors":[{"detail":"This operation is only supported for datasets with providers [\'nexgddp\', \'loca\']","status":422}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_query_dataset_invalid_connector_type(client, mocker):
    mock_get_dataset(mocker, dataset_invalid_connector_type)
    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))

    response = client.get(
        '/api/v1/nexgddp/query/bar', headers={'x-api-key': 'api-key-test'}
    )
    assert response.status_code == 422
    assert response.data == b'{"errors":[{"detail":"This operation is only supported for datasets with connectorType \'rest\'","status":422}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_query_dataset_no_query(client, mocker):
    mock_get_dataset(mocker, dataset_nexgddp_json)
    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))

    response = client.get(
        '/api/v1/nexgddp/query/bar', headers={'x-api-key': 'api-key-test'}
    )
    assert response.status_code == 400
    assert response.data == b'{"errors":[{"detail":"sql must be provided","status":400}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_query_dataset_happy_case_nexgddp(client, mocker):
    query_json = {
        "data": {
            "type": "result",
            "attributes": {
                "query": "SELECT * FROM data",
                "jsonSql": {
                    "select": [
                        {
                            "value": "*",
                            "alias": None,
                            "type": "wildcard"
                        }
                    ],
                    "from": "data"
                }
            }
        }
    }

    geostore = {
        "data": {
            "type": "geoStore",
            "id": "46e0617e8d2000bd3c36e9e92bb5a35b",
            "attributes": {
                "geojson": {
                    "crs": {},
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [
                                            -119.7565,
                                            37.0267
                                        ],
                                        [
                                            -120.314,
                                            36.7147
                                        ],
                                        [
                                            -119.707,
                                            36.6728
                                        ],
                                        [
                                            -119.7565,
                                            37.0267
                                        ]
                                    ]
                                ]
                            }
                        }
                    ]
                },
                "hash": "46e0617e8d2000bd3c36e9e92bb5a35b",
                "provider": {},
                "areaHa": 105478.36213012414,
                "bbox": [
                    -120.314,
                    36.6728,
                    -119.707,
                    37.0267
                ],
                "lock": False,
                "info": {
                    "use": {}
                }
            }
        }
    }

    mock_get_dataset(mocker, dataset_nexgddp_json)
    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))

    rasdaman_response_file = Path("tests/assets/get_fields_response.xml")
    with open(rasdaman_response_file, encoding='utf-8') as f:
        rasdaman_response = f.read()

        mocker.get(
            '{}?SERVICE=WCS&VERSION=2.0.1&REQUEST=DescribeCoverage&COVERAGEID=cum_pr_rcp45_decadal_processed'.format(
                os.getenv('RASDAMAN_URL')),
            text=rasdaman_response)

        mocker.get(
            '{}/v1/convert/sql2SQL?sql=select%20%2A%20from%20data'.format(
                os.getenv('GATEWAY_URL')),
            json=query_json)

        mocker.get(
            '{}/v1/geostore/46e0617e8d2000bd3c36e9e92bb5a35b'.format(
                os.getenv('GATEWAY_URL')),
            json=geostore)

        mocker.post(
            os.getenv('RASDAMAN_URL'),
            text='"0.01509484 0.01570915 0.0162086","0.01515527 0.01574764 0.01613505","0.01569834 0.01584321 0.01604243","0.01529838 0.01577098 0.01628494","0.01496018 0.01581504 0.01673895","0.01516556 0.01605528 0.01679384","0.01520149 0.01590445 0.01696718","0.01500688 0.01575057 0.01687931","0.01521986 0.01596653 0.01714825","0.01444959 0.01618101 0.01762878","0.0152118 0.01630418 0.01758959","0.01550237 0.01636603 0.01747726"'
        )

        response = client.get(
            '/api/v1/nexgddp/query/bar?sql=select%20%2A%20from%20data&geostore=46e0617e8d2000bd3c36e9e92bb5a35b',
            headers={'x-api-key': 'api-key-test'}
        )

        assert response.status_code == 200
        assert response.data == b'{"data":[{"cum_pr":0.01570915,"cum_pr_q25":0.01509484,"cum_pr_q75":0.0162086,"year":"1971-01-01T00:00:00-01:00"},{"cum_pr":0.01574764,"cum_pr_q25":0.01515527,"cum_pr_q75":0.01613505,"year":"1981-01-01T00:00:00-01:00"},{"cum_pr":0.01584321,"cum_pr_q25":0.01569834,"cum_pr_q75":0.01604243,"year":"1991-01-01T00:00:00-01:00"},{"cum_pr":0.01577098,"cum_pr_q25":0.01529838,"cum_pr_q75":0.01628494,"year":"2001-01-01T00:00:00-01:00"},{"cum_pr":0.01581504,"cum_pr_q25":0.01496018,"cum_pr_q75":0.01673895,"year":"2011-01-01T00:00:00-01:00"},{"cum_pr":0.01605528,"cum_pr_q25":0.01516556,"cum_pr_q75":0.01679384,"year":"2021-01-01T00:00:00-01:00"},{"cum_pr":0.01590445,"cum_pr_q25":0.01520149,"cum_pr_q75":0.01696718,"year":"2031-01-01T00:00:00-01:00"},{"cum_pr":0.01575057,"cum_pr_q25":0.01500688,"cum_pr_q75":0.01687931,"year":"2041-01-01T00:00:00-01:00"},{"cum_pr":0.01596653,"cum_pr_q25":0.01521986,"cum_pr_q75":0.01714825,"year":"2051-01-01T00:00:00-01:00"},{"cum_pr":0.01618101,"cum_pr_q25":0.01444959,"cum_pr_q75":0.01762878,"year":"2061-01-01T00:00:00-01:00"},{"cum_pr":0.01630418,"cum_pr_q25":0.0152118,"cum_pr_q75":0.01758959,"year":"2071-01-01T00:00:00-01:00"},{"cum_pr":0.01636603,"cum_pr_q25":0.01550237,"cum_pr_q75":0.01747726,"year":"2081-01-01T00:00:00-01:00"}]}\n'



@requests_mock.Mocker(kw='mocker')
def test_query_dataset_happy_case_loca(client, mocker):
    query_json = {
        "data": {
            "type": "result",
            "attributes": {
                "query": "SELECT * FROM data",
                "jsonSql": {
                    "select": [
                        {
                            "value": "*",
                            "alias": None,
                            "type": "wildcard"
                        }
                    ],
                    "from": "data"
                }
            }
        }
    }

    geostore = {
        "data": {
            "type": "geoStore",
            "id": "46e0617e8d2000bd3c36e9e92bb5a35b",
            "attributes": {
                "geojson": {
                    "crs": {},
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [
                                        [
                                            -119.7565,
                                            37.0267
                                        ],
                                        [
                                            -120.314,
                                            36.7147
                                        ],
                                        [
                                            -119.707,
                                            36.6728
                                        ],
                                        [
                                            -119.7565,
                                            37.0267
                                        ]
                                    ]
                                ]
                            }
                        }
                    ]
                },
                "hash": "46e0617e8d2000bd3c36e9e92bb5a35b",
                "provider": {},
                "areaHa": 105478.36213012414,
                "bbox": [
                    -120.314,
                    36.6728,
                    -119.707,
                    37.0267
                ],
                "lock": False,
                "info": {
                    "use": {}
                }
            }
        }
    }

    mock_get_dataset(mocker, dataset_loca_json)
    mock_request_validation(mocker, microservice_token=os.getenv("MICROSERVICE_TOKEN"))

    rasdaman_response_file = Path("tests/assets/get_fields_response.xml")
    with open(rasdaman_response_file, encoding='utf-8') as f:
        rasdaman_response = f.read()

        mocker.get(
            '{}?SERVICE=WCS&VERSION=2.0.1&REQUEST=DescribeCoverage&COVERAGEID=cum_pr_rcp45_decadal_processed'.format(
                os.getenv('RASDAMAN_URL')),
            text=rasdaman_response)

        mocker.get(
            '{}/v1/convert/sql2SQL?sql=select%20%2A%20from%20data'.format(
                os.getenv('GATEWAY_URL')),
            json=query_json)

        mocker.get(
            '{}/v1/geostore/46e0617e8d2000bd3c36e9e92bb5a35b'.format(
                os.getenv('GATEWAY_URL')),
            json=geostore)

        mocker.post(
            os.getenv('RASDAMAN_URL'),
            text='"0.01509484 0.01570915 0.0162086","0.01515527 0.01574764 0.01613505","0.01569834 0.01584321 0.01604243","0.01529838 0.01577098 0.01628494","0.01496018 0.01581504 0.01673895","0.01516556 0.01605528 0.01679384","0.01520149 0.01590445 0.01696718","0.01500688 0.01575057 0.01687931","0.01521986 0.01596653 0.01714825","0.01444959 0.01618101 0.01762878","0.0152118 0.01630418 0.01758959","0.01550237 0.01636603 0.01747726"'
        )

        response = client.get(
            '/api/v1/nexgddp/query/bar?sql=select%20%2A%20from%20data&geostore=46e0617e8d2000bd3c36e9e92bb5a35b',
            headers={'x-api-key': 'api-key-test'}
        )

        assert response.status_code == 200
        assert response.data == b'{"data":[{"cum_pr":0.01570915,"cum_pr_q25":0.01509484,"cum_pr_q75":0.0162086,"year":"1971-01-01T00:00:00-01:00"},{"cum_pr":0.01574764,"cum_pr_q25":0.01515527,"cum_pr_q75":0.01613505,"year":"1981-01-01T00:00:00-01:00"},{"cum_pr":0.01584321,"cum_pr_q25":0.01569834,"cum_pr_q75":0.01604243,"year":"1991-01-01T00:00:00-01:00"},{"cum_pr":0.01577098,"cum_pr_q25":0.01529838,"cum_pr_q75":0.01628494,"year":"2001-01-01T00:00:00-01:00"},{"cum_pr":0.01581504,"cum_pr_q25":0.01496018,"cum_pr_q75":0.01673895,"year":"2011-01-01T00:00:00-01:00"},{"cum_pr":0.01605528,"cum_pr_q25":0.01516556,"cum_pr_q75":0.01679384,"year":"2021-01-01T00:00:00-01:00"},{"cum_pr":0.01590445,"cum_pr_q25":0.01520149,"cum_pr_q75":0.01696718,"year":"2031-01-01T00:00:00-01:00"},{"cum_pr":0.01575057,"cum_pr_q25":0.01500688,"cum_pr_q75":0.01687931,"year":"2041-01-01T00:00:00-01:00"},{"cum_pr":0.01596653,"cum_pr_q25":0.01521986,"cum_pr_q75":0.01714825,"year":"2051-01-01T00:00:00-01:00"},{"cum_pr":0.01618101,"cum_pr_q25":0.01444959,"cum_pr_q75":0.01762878,"year":"2061-01-01T00:00:00-01:00"},{"cum_pr":0.01630418,"cum_pr_q25":0.0152118,"cum_pr_q75":0.01758959,"year":"2071-01-01T00:00:00-01:00"},{"cum_pr":0.01636603,"cum_pr_q25":0.01550237,"cum_pr_q75":0.01747726,"year":"2081-01-01T00:00:00-01:00"}]}\n'
