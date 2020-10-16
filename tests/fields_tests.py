import pytest
import os
import requests_mock

import nexgddp
from pathlib import Path

@pytest.fixture
def client():
    app = nexgddp.app
    app.config['TESTING'] = True
    client = app.test_client()

    yield client


@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_that_does_not_exist(client, mocker):
    mocker.get('{}/v1/dataset/foo'.format(os.getenv('CT_URL')), status_code=404,
               json={"errors": [{"status": 404, "detail": "Dataset with id 'foo' doesn't exist"}]})

    response = client.post('/api/v1/nexgddp/fields/foo')
    assert response.status_code == 404
    assert response.data == b'{"errors":[{"detail":"Dataset with id foo doesn\'t exist","status":404}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_invalid_provider(client, mocker):
    dataset_json = {
        'data': {
            'id': 'bar',
            'type': 'dataset',
            'attributes': {
                'name': 'Test dataset 1',
                'slug': 'test-dataset-1',
                'type': 'tabular',
                'subtitle': None,
                'application': [
                    'rw'
                ],
                'dataPath': None,
                'attributesPath': None,
                'connectorType': 'rest',
                'provider': 'csv',
                'userId': '1',
                'connectorUrl': 'https://raw.githubusercontent.com/test/file.csv',
                'sources': [],
                'tableName': 'index_d1ced4227cd5480a8904d3410d75bf42_1587619728489',
                'status': 'saved',
                'published': False,
                'overwrite': True,
                'mainDateField': None,
                'env': 'production',
                'geoInfo': False,
                'protected': False,
                'clonedHost': {},
                'legend': {},
                'errorMessage': None,
                'taskId': None,
                'createdAt': '2016-08-01T15:28:15.050Z',
                'updatedAt': '2018-01-05T18:15:23.266Z',
                'dataLastUpdated': None,
                'widgetRelevantProps': [],
                'layerRelevantProps': []
            }
        }}

    mocker.get('{}/v1/dataset/bar'.format(os.getenv('CT_URL')), json=dataset_json)

    response = client.post('/api/v1/nexgddp/fields/bar')
    assert response.status_code == 422
    assert response.data == b'{"errors":[{"detail":"This operation is only supported for datasets with provider \'nexgddp\'","status":422}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_invalid_connector_type(client, mocker):
    dataset_json = {
        'data': {
            'id': 'bar',
            'type': 'dataset',
            'attributes': {
                'name': 'Test dataset 1',
                'slug': 'test-dataset-1',
                'type': 'tabular',
                'subtitle': None,
                'application': [
                    'rw'
                ],
                'dataPath': None,
                'attributesPath': None,
                'connectorType': 'document',
                'provider': 'nexgddp',
                'userId': '1',
                'connectorUrl': None,
                'sources': [],
                'tableName': 'cum_pr/rcp45_decadal',
                'status': 'saved',
                'published': False,
                'overwrite': True,
                'mainDateField': None,
                'env': 'production',
                'geoInfo': False,
                'protected': False,
                'clonedHost': {},
                'legend': {},
                'errorMessage': None,
                'taskId': None,
                'createdAt': '2016-08-01T15:28:15.050Z',
                'updatedAt': '2018-01-05T18:15:23.266Z',
                'dataLastUpdated': None,
                'widgetRelevantProps': [],
                'layerRelevantProps': []
            }
        }}

    mocker.get('{}/v1/dataset/bar'.format(os.getenv('CT_URL')), json=dataset_json)

    response = client.post('/api/v1/nexgddp/fields/bar')
    assert response.status_code == 422
    assert response.data == b'{"errors":[{"detail":"This operation is only supported for datasets with connectorType \'rest\'","status":422}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_happy_case(client, mocker):
    dataset_json = {
        'data': {
            'id': 'bar',
            'type': 'dataset',
            'attributes': {
                'name': 'Test dataset 1',
                'slug': 'test-dataset-1',
                'type': 'tabular',
                'subtitle': None,
                'application': [
                    'rw'
                ],
                'dataPath': None,
                'attributesPath': None,
                'connectorType': 'rest',
                'provider': 'nexgddp',
                'userId': '1',
                'connectorUrl': None,
                'sources': [],
                'tableName': 'cum_pr/rcp45_decadal',
                'status': 'saved',
                'published': False,
                'overwrite': True,
                'mainDateField': None,
                'env': 'production',
                'geoInfo': False,
                'protected': False,
                'clonedHost': {},
                'legend': {},
                'errorMessage': None,
                'taskId': None,
                'createdAt': '2016-08-01T15:28:15.050Z',
                'updatedAt': '2018-01-05T18:15:23.266Z',
                'dataLastUpdated': None,
                'widgetRelevantProps': [],
                'layerRelevantProps': []
            }
        }}

    rasdaman_response_file = Path("tests/assets/get_fields_response.xml")
    with open(rasdaman_response_file, encoding='utf-8') as f:
        rasdaman_response = f.read()

        mocker.get('{}?SERVICE=WCS&VERSION=2.0.1&REQUEST=DescribeCoverage&COVERAGEID=cum_pr_rcp45_decadal_processed'.format(os.getenv('RASDAMAN_URL')),
                   text=rasdaman_response)

        mocker.get('{}/v1/dataset/bar'.format(os.getenv('CT_URL')), json=dataset_json)

        response = client.post('/api/v1/nexgddp/fields/bar')
        assert response.status_code == 200
        assert response.data == b'{"fields":{"cum_pr":{"type":"number","uom":"10^0"},"cum_pr_q25":{"type":"number","uom":"10^0"},"cum_pr_q75":{"type":"number","uom":"10^0"},"year":{"type":"date"}},"tableName":"cum_pr/rcp45_decadal"}\n'
