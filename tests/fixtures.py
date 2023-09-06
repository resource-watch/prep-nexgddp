import copy

dataset_loca_json = {
    "data": {
        "id": "bar",
        "type": "dataset",
        "attributes": {
            "name": "Test dataset 1",
            "slug": "test-dataset-1",
            "type": "tabular",
            "subtitle": None,
            "application": ["rw"],
            "dataPath": None,
            "attributesPath": None,
            "connectorType": "rest",
            "provider": "loca",
            "userId": "1",
            "connectorUrl": None,
            "sources": [],
            "tableName": "cum_pr/rcp45_decadal",
            "status": "saved",
            "published": False,
            "overwrite": True,
            "mainDateField": None,
            "env": "production",
            "geoInfo": False,
            "protected": False,
            "clonedHost": {},
            "legend": {},
            "errorMessage": None,
            "taskId": None,
            "createdAt": "2016-08-01T15:28:15.050Z",
            "updatedAt": "2018-01-05T18:15:23.266Z",
            "dataLastUpdated": None,
            "widgetRelevantProps": [],
            "layerRelevantProps": [],
        },
    }
}

dataset_nexgddp_json = copy.deepcopy(dataset_loca_json)
dataset_nexgddp_json["data"]["attributes"].update(provider="nexgddp")

dataset_invalid_provider = copy.deepcopy(dataset_loca_json)
dataset_invalid_provider["data"]["attributes"].update(provider="csv")

dataset_invalid_connector_type = copy.deepcopy(dataset_loca_json)
dataset_invalid_connector_type["data"]["attributes"].update(connectorType="document")

USERS = {
    "ADMIN": {
        "id": "1a10d7c6e0a37126611fd7a7",
        "role": "ADMIN",
        "provider": "local",
        "email": "user@control-tower.org",
        "name": "John Admin",
        "extraUserData": {
            "apps": [
                "rw",
                "gfw",
                "gfw-climate",
                "prep",
                "aqueduct",
                "forest-atlas",
                "data4sdgs",
            ]
        },
    },
    "MANAGER": {
        "id": "1a10d7c6e0a37126611fd7a7",
        "role": "MANAGER",
        "provider": "local",
        "name": "John Manager",
        "email": "user@control-tower.org",
        "extraUserData": {
            "apps": [
                "rw",
                "gfw",
                "gfw-climate",
                "prep",
                "aqueduct",
                "forest-atlas",
                "data4sdgs",
            ]
        },
    },
    "USER": {
        "id": "1a10d7c6e0a37126611fd7a7",
        "role": "USER",
        "name": "John User",
        "provider": "local",
        "email": "user@control-tower.org",
        "extraUserData": {
            "apps": [
                "rw",
                "gfw",
                "gfw-climate",
                "prep",
                "aqueduct",
                "forest-atlas",
                "data4sdgs",
            ]
        },
    },
    "MICROSERVICE": {"id": "microservice", "createdAt": "2016-09-14"},
}
