import os


def mock_get_dataset(mocker, response_json, status_code=200):
    mocker.get(
        f"{os.getenv('GATEWAY_URL')}/v1/dataset/bar",
        request_headers={
            "content-type": "application/json",
            # "x-api-key": "api-key-test",
        },
        status_code=status_code,
        json=response_json,
    )
