import json

import pytest
import requests_mock

import nexgddp

USERS = {
    "ADMIN": {
        "id": '1a10d7c6e0a37126611fd7a7',
        "role": 'ADMIN',
        "provider": 'local',
        "email": 'user@control-tower.org',
        "name": 'John Admin',
        "extraUserData": {
            "apps": [
                'rw',
                'gfw',
                'gfw-climate',
                'prep',
                'aqueduct',
                'forest-atlas',
                'data4sdgs'
            ]
        }
    },
    "MANAGER": {
        "id": '1a10d7c6e0a37126611fd7a7',
        "role": 'MANAGER',
        "provider": 'local',
        "email": 'user@control-tower.org',
        "extraUserData": {
            "apps": [
                'rw',
                'gfw',
                'gfw-climate',
                'prep',
                'aqueduct',
                'forest-atlas',
                'data4sdgs'
            ]
        }
    },
    "USER": {
        "id": '1a10d7c6e0a37126611fd7a7',
        "role": 'USER',
        "provider": 'local',
        "email": 'user@control-tower.org',
        "extraUserData": {
            "apps": [
                'rw',
                'gfw',
                'gfw-climate',
                'prep',
                'aqueduct',
                'forest-atlas',
                'data4sdgs'
            ]
        }
    },
    "MICROSERVICE": {
        "id": "microservice",
        "createdAt": "2016-09-14"
    }
}


@pytest.fixture
def client():
    app = nexgddp.app
    app.config['TESTING'] = True
    client = app.test_client()

    yield client


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_anon(client, mocker):
    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache')
    assert json.loads(response.data) == {'errors': [{'detail': 'Not authorized', 'status': 403}]}
    assert response.status_code == 403


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_as_admin(client, mocker):
    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache?loggedUser={}'.format(json.dumps(USERS['ADMIN'])))
    assert json.loads(response.data) == {'errors': [{'detail': 'Not authorized', 'status': 403}]}
    assert response.status_code == 403


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_as_manager(client, mocker):
    response = client.delete(
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache?loggedUser={}'.format(json.dumps(USERS['MANAGER'])))
    assert json.loads(response.data) == {'errors': [{'detail': 'Not authorized', 'status': 403}]}
    assert response.status_code == 403


@requests_mock.Mocker(kw='mocker')
def test_expire_layer_cache_happy_case_for_empty_cache(client, mocker):
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
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache?loggedUser={}'.format(json.dumps(USERS['MICROSERVICE'])))
    assert json.loads(response.data) == {'result': 'OK'}
    assert response.status_code == 200


@requests_mock.Mocker(kw='mocker')
def test_expire_nexgddp_layer_cache_happy_case(client, mocker):
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
        '/api/v1/nexgddp/layer/nexgddp/:layer/expire-cache?loggedUser={}'.format(json.dumps(USERS['MICROSERVICE'])))
    assert json.loads(response.data) == {'result': 'OK'}
    assert response.status_code == 200


@requests_mock.Mocker(kw='mocker')
def test_expire_loca_layer_cache_happy_case(client, mocker):
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
        '/api/v1/nexgddp/layer/loca/:layer/expire-cache?loggedUser={}'.format(json.dumps(USERS['MICROSERVICE'])))
    assert json.loads(response.data) == {'result': 'OK'}
    assert response.status_code == 200
