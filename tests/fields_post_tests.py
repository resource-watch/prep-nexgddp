import os
import requests_mock

from pathlib import Path

from tests.mocks import mock_get_dataset
from tests.fixtures import (
    dataset_loca_json,
    dataset_nexgddp_json,
    dataset_invalid_connector_type,
    dataset_invalid_provider,
)

@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_that_does_not_exist(client, mocker):
    mock_get_dataset(
        mocker,
        status_code=404,
        response_json={"errors": [{"status": 404, "detail": "Dataset with id 'bar' doesn't exist"}]}
    )
    response = client.post('/api/v1/nexgddp/fields/bar')
    assert response.status_code == 404
    assert response.data == b'{"errors":[{"detail":"Dataset with id bar doesn\'t exist","status":404}]}\n'

@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_invalid_provider(client, mocker):
    mock_get_dataset(mocker, dataset_invalid_provider)

    response = client.post('/api/v1/nexgddp/fields/bar')
    assert response.status_code == 422
    assert response.data == b'{"errors":[{"detail":"This operation is only supported for datasets with providers [\'nexgddp\', \'loca\']","status":422}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_invalid_connector_type(client, mocker):
    mock_get_dataset(mocker, dataset_invalid_connector_type)

    response = client.post('/api/v1/nexgddp/fields/bar')
    assert response.status_code == 422
    assert response.data == b'{"errors":[{"detail":"This operation is only supported for datasets with connectorType \'rest\'","status":422}]}\n'


@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_happy_case_nexgddp(client, mocker):
    mock_get_dataset(mocker, dataset_nexgddp_json)

    rasdaman_response_file = Path("tests/assets/get_fields_response.xml")
    with open(rasdaman_response_file, encoding='utf-8') as f:
        rasdaman_response = f.read()

        mocker.get('{}?SERVICE=WCS&VERSION=2.0.1&REQUEST=DescribeCoverage&COVERAGEID=cum_pr_rcp45_decadal_processed'.format(os.getenv('RASDAMAN_URL')),
                   text=rasdaman_response)
        mock_get_dataset(mocker, dataset_nexgddp_json)

        response = client.post('/api/v1/nexgddp/fields/bar', json={})
        assert response.status_code == 200
        assert response.data == b'{"fields":{"cum_pr":{"type":"number","uom":"10^0"},"cum_pr_q25":{"type":"number","uom":"10^0"},"cum_pr_q75":{"type":"number","uom":"10^0"},"year":{"type":"date"}},"tableName":"cum_pr/rcp45_decadal"}\n'


@requests_mock.Mocker(kw='mocker')
def test_get_fields_for_dataset_happy_case_loca(client, mocker):

    rasdaman_response_file = Path("tests/assets/get_fields_response.xml")
    with open(rasdaman_response_file, encoding='utf-8') as f:
        rasdaman_response = f.read()

        mocker.get('{}?SERVICE=WCS&VERSION=2.0.1&REQUEST=DescribeCoverage&COVERAGEID=cum_pr_rcp45_decadal_processed'.format(os.getenv('RASDAMAN_URL')),
                   text=rasdaman_response)
        mock_get_dataset(mocker, dataset_loca_json)

        response = client.post('/api/v1/nexgddp/fields/bar', json={})
        assert response.status_code == 200
        assert response.data == b'{"fields":{"cum_pr":{"type":"number","uom":"10^0"},"cum_pr_q25":{"type":"number","uom":"10^0"},"cum_pr_q75":{"type":"number","uom":"10^0"},"year":{"type":"date"}},"tableName":"cum_pr/rcp45_decadal"}\n'
