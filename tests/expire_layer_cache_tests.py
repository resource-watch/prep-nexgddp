import json
import os

import requests_mock
from RWAPIMicroservicePython.test_utils import mock_request_validation

from tests.fixtures import USERS


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_anon(client, mocker):
    mock_request_validation(
            mocker,
            microservice_token=os.getenv("MICROSERVICE_TOKEN"),
        )
    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache',
        headers={'x-api-key': 'api-key-test'}
    )
    assert json.loads(response.data) == {'errors': [{'detail': 'Not authorized', 'status': 403}]}
    assert response.status_code == 403


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_as_admin(client, mocker):
    get_user_data_calls = mock_request_validation(
        mocker,
        microservice_token=os.getenv("MICROSERVICE_TOKEN"),
        user=USERS["ADMIN"]
    )

    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache',
        headers={'Authorization': 'Bearer abcd', 'x-api-key': 'api-key-test'}
    )
    assert json.loads(response.data) == {'errors': [{'detail': 'Not authorized', 'status': 403}]}
    assert response.status_code == 403
    assert get_user_data_calls.called
    assert get_user_data_calls.call_count == 1


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_as_manager(client, mocker):
    get_user_data_calls = mock_request_validation(
        mocker,
        microservice_token=os.getenv("MICROSERVICE_TOKEN"),
        user=USERS["MANAGER"]
    )

    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache',
        headers={'Authorization': 'Bearer abcd', 'x-api-key': 'api-key-test'}
    )
    assert json.loads(response.data) == {'errors': [{'detail': 'Not authorized', 'status': 403}]}
    assert response.status_code == 403
    assert get_user_data_calls.called
    assert get_user_data_calls.call_count == 1


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_happy_case_for_empty_cache(client, mocker):
    get_user_data_calls = mock_request_validation(
        mocker,
        microservice_token=os.getenv("MICROSERVICE_TOKEN"),
        user=USERS["MICROSERVICE"]
    )

    mocker.post('https://accounts.google.com/o/oauth2/token', json={
        'access_token': 'TEST_GOOGLE_OAUTH2_ACCESS_TOKEN',
        'expires_in': 3599,
        'scope': 'openid https://www.googleapis.com/auth/userinfo.email',
        'token_type': 'Bearer',
        'id_token': 'some_id_token'
    })

    mocker.get('https://www.googleapis.com/storage/v1/b/nexgddp-tiles?projection=noAcl', json={})
    mocker.get('https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o?prefix=%3Alayer&projection=noAcl', json={})

    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache',
        headers={'Authorization': 'Bearer abcd', 'x-api-key': 'api-key-test'}
    )
    assert json.loads(response.data) == {'result': 'OK'}
    assert response.status_code == 200
    assert get_user_data_calls.called
    assert get_user_data_calls.call_count == 1


@requests_mock.Mocker(kw='mocker')
def test_expire_nexgddp_layer_cache_happy_case(client, mocker):
    get_user_data_calls = mock_request_validation(
        mocker,
        microservice_token=os.getenv("MICROSERVICE_TOKEN"),
        user=USERS["MICROSERVICE"]
    )

    mocker.post('https://accounts.google.com/o/oauth2/token', json={
        'access_token': 'TEST_GOOGLE_OAUTH2_ACCESS_TOKEN',
        'expires_in': 3599,
        'scope': 'openid https://www.googleapis.com/auth/userinfo.email',
        'token_type': 'Bearer',
        'id_token': 'some_id_token'
    })

    mocker.get('https://www.googleapis.com/storage/v1/b/nexgddp-tiles?projection=noAcl',
               json={"kind": "storage#bucket", "id": "gee-tiles",
                     "selfLink": "https://www.googleapis.com/storage/v1/b/gee-tiles", "projectNumber": "123456",
                     "name": "gee-tiles", "timeCreated": "2017-09-06T16:55:16.193Z",
                     "updated": "2018-02-05T11:12:48.289Z", "metageneration": "2", "location": "US",
                     "locationType": "multi-region", "cors": [
                       {"origin": ["*"], "method": ["GET", "HEAD", "DELETE"], "responseHeader": ["Content-Type"],
                        "maxAgeSeconds": 3600}], "storageClass": "MULTI_REGIONAL", "etag": "CAI="})

    mocker.get('https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o?prefix=%3Alayer&projection=noAcl',
               json={'kind': 'storage#objects', 'items': [{'kind': 'storage#object',
                                                           'id': 'nexgddp-tiles/testLayerId/9/169/283/tile_854ffa01c65fbe214c1587f9308e77a6.png/1522853136643054',
                                                           'selfLink': 'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F283%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'name': 'testLayerId/9/169/283/tile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'bucket': 'nexgddp-tiles', 'generation': '1522853136643054',
                                                           'metageneration': '2',
                                                           'contentType': 'application/octet-stream',
                                                           'storageClass': 'MULTI_REGIONAL', 'size': '34199',
                                                           'md5Hash': 'Mlp3RxXNXA2KtN8Oc2Lgdw==',
                                                           'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F283%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png?generation=1522853136643054&alt=media',
                                                           'crc32c': 'kwKCGw==', 'etag': 'CO7nhILuoNoCEAI=',
                                                           'timeCreated': '2018-04-04T14:45:36.600Z',
                                                           'updated': '2018-04-04T14:45:36.885Z',
                                                           'timeStorageClassUpdated': '2018-04-04T14:45:36.600Z'},
                                                          {'kind': 'storage#object',
                                                           'id': 'nexgddp-tiles/testLayerId/9/169/284/tile_854ffa01c65fbe214c1587f9308e77a6.png/1522853136686208',
                                                           'selfLink': 'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F284%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'name': 'testLayerId/9/169/284/tile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'bucket': 'nexgddp-tiles', 'generation': '1522853136686208',
                                                           'metageneration': '2',
                                                           'contentType': 'application/octet-stream',
                                                           'storageClass': 'MULTI_REGIONAL', 'size': '32750',
                                                           'md5Hash': 'GP2kzks/qwzISVHQTCHPzg==',
                                                           'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F284%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png?generation=1522853136686208&alt=media',
                                                           'crc32c': '4DvHHw==', 'etag': 'CIC5h4LuoNoCEAI=',
                                                           'timeCreated': '2018-04-04T14:45:36.645Z',
                                                           'updated': '2018-04-04T14:45:36.981Z',
                                                           'timeStorageClassUpdated': '2018-04-04T14:45:36.645Z'}]})

    mocker.delete(
        'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F283%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
        status_code=204
    )

    mocker.delete(
        'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F284%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
        status_code=204
    )

    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache',
        headers={'Authorization': 'Bearer abcd', 'x-api-key': 'api-key-test'}
    )
    assert json.loads(response.data) == {'result': 'OK'}
    assert response.status_code == 200
    assert get_user_data_calls.called
    assert get_user_data_calls.call_count == 1


@requests_mock.Mocker(kw='mocker')
def test_expire_loca_layer_cache_happy_case(client, mocker):
    get_user_data_calls = mock_request_validation(
        mocker,
        microservice_token=os.getenv("MICROSERVICE_TOKEN"),
        user=USERS["MICROSERVICE"]
    )

    mocker.post('https://accounts.google.com/o/oauth2/token', json={
        'access_token': 'TEST_GOOGLE_OAUTH2_ACCESS_TOKEN',
        'expires_in': 3599,
        'scope': 'openid https://www.googleapis.com/auth/userinfo.email',
        'token_type': 'Bearer',
        'id_token': 'some_id_token'
    })

    mocker.get('https://www.googleapis.com/storage/v1/b/nexgddp-tiles?projection=noAcl',
               json={"kind": "storage#bucket", "id": "gee-tiles",
                     "selfLink": "https://www.googleapis.com/storage/v1/b/gee-tiles", "projectNumber": "123456",
                     "name": "gee-tiles", "timeCreated": "2017-09-06T16:55:16.193Z",
                     "updated": "2018-02-05T11:12:48.289Z", "metageneration": "2", "location": "US",
                     "locationType": "multi-region", "cors": [
                       {"origin": ["*"], "method": ["GET", "HEAD", "DELETE"], "responseHeader": ["Content-Type"],
                        "maxAgeSeconds": 3600}], "storageClass": "MULTI_REGIONAL", "etag": "CAI="})

    mocker.get('https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o?prefix=%3Alayer&projection=noAcl',
               json={'kind': 'storage#objects', 'items': [{'kind': 'storage#object',
                                                           'id': 'nexgddp-tiles/testLayerId/9/169/283/tile_854ffa01c65fbe214c1587f9308e77a6.png/1522853136643054',
                                                           'selfLink': 'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F283%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'name': 'testLayerId/9/169/283/tile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'bucket': 'nexgddp-tiles', 'generation': '1522853136643054',
                                                           'metageneration': '2',
                                                           'contentType': 'application/octet-stream',
                                                           'storageClass': 'MULTI_REGIONAL', 'size': '34199',
                                                           'md5Hash': 'Mlp3RxXNXA2KtN8Oc2Lgdw==',
                                                           'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F283%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png?generation=1522853136643054&alt=media',
                                                           'crc32c': 'kwKCGw==', 'etag': 'CO7nhILuoNoCEAI=',
                                                           'timeCreated': '2018-04-04T14:45:36.600Z',
                                                           'updated': '2018-04-04T14:45:36.885Z',
                                                           'timeStorageClassUpdated': '2018-04-04T14:45:36.600Z'},
                                                          {'kind': 'storage#object',
                                                           'id': 'nexgddp-tiles/testLayerId/9/169/284/tile_854ffa01c65fbe214c1587f9308e77a6.png/1522853136686208',
                                                           'selfLink': 'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F284%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'name': 'testLayerId/9/169/284/tile_854ffa01c65fbe214c1587f9308e77a6.png',
                                                           'bucket': 'nexgddp-tiles', 'generation': '1522853136686208',
                                                           'metageneration': '2',
                                                           'contentType': 'application/octet-stream',
                                                           'storageClass': 'MULTI_REGIONAL', 'size': '32750',
                                                           'md5Hash': 'GP2kzks/qwzISVHQTCHPzg==',
                                                           'mediaLink': 'https://www.googleapis.com/download/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F284%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png?generation=1522853136686208&alt=media',
                                                           'crc32c': '4DvHHw==', 'etag': 'CIC5h4LuoNoCEAI=',
                                                           'timeCreated': '2018-04-04T14:45:36.645Z',
                                                           'updated': '2018-04-04T14:45:36.981Z',
                                                           'timeStorageClassUpdated': '2018-04-04T14:45:36.645Z'}]})

    mocker.delete(
        'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F283%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
        status_code=204
    )

    mocker.delete(
        'https://www.googleapis.com/storage/v1/b/nexgddp-tiles/o/testLayerId%2F9%2F169%2F284%2Ftile_854ffa01c65fbe214c1587f9308e77a6.png',
        status_code=204
    )

    response = client.delete(
        '/api/v1/nexgddp/layer/loca/:layer/expire-cache',
        headers={'Authorization': 'Bearer abcd', 'x-api-key': 'api-key-test'}
    )
    assert json.loads(response.data) == {'result': 'OK'}
    assert response.status_code == 200
    assert get_user_data_calls.called
    assert get_user_data_calls.call_count == 1
