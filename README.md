# PREP - NEXGDDP microservice

[![Build Status](https://travis-ci.com/Vizzuality/prep-nexgddp.svg?branch=dev)](https://travis-ci.com/Vizzuality/prep-nexgddp)
[![Test Coverage](https://api.codeclimate.com/v1/badges/95cbefa669563539f187/test_coverage)](https://codeclimate.com/github/Vizzuality/prep-nexgddp/test_coverage)

## Dependencies

Dependencies on other Microservices:
- [Dataset](https://github.com/resource-watch/dataset/)
- [Geostore](https://github.com/gfw-api/gfw-geostore-api)
- [GFW OGR](https://github.com/gfw-api/gfw-ogr-api)
- [Layer](https://github.com/resource-watch/layer)
- [Query](https://github.com/resource-watch/query/)

## Getting started

### Requirements

You need to install Docker in your machine if you haven't already [Docker](https://www.docker.com/)

### Development

Follow the next steps to set up the development environment in your machine.

1. Clone the repo and go to the folder

```ssh
git clone https://github.com/Vizzuality/nex-gddp
cd nex-gddp
```

2. Run the ms.sh shell script in development mode.

```ssh
./nexgddp.sh develop
```

If this is the first time you run it, it may take a few minutes.

### Code structure

The API has been packed in a Python module (ps). It creates and exposes a WSGI application. The core functionality
has been divided in three different layers or submodules (Routes, Services and Models).

There are also some generic submodules that manage the request validations, HTTP errors and the background tasks manager.


## Tests

As this microservice relies on Google Earth Engine, tests require a valid `storage.json` or equivalent file. 
At the time of this writing, actual tests use mock calls, so the real credential are only needed because Google's 
library actually validates the credentials on startup. 

Before you run the tests, be sure to install the necessary development libraries, using `pip install -r requirements_dev.txt`.

Actual test execution is done by running the `pytest` executable on the root of the project.  
