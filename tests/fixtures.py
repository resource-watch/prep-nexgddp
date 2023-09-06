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
