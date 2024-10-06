# platform-management-v2

This is a temporary utility for data manipulation of IDU Digital City Platform for the time until idu-uploader is fully functional.

Urban API can be obtained [here](https://github.com/iduprojects/idu_api) as a part of idu-api

## Installation

1. Install Python3 and poetry
2. Clone this repo
3. Use `pip install` for installation or `poetry install` and `poetry run pmv2 --help` to use in repository directory

## Available commands

### list-territories

Print territories hierarchy up to given level. Realization is really slow, there will be one API call executed sequentially for each territory of given level limit (if set).

### list-service-types, list-physical-object-types

Print list of correcponding entities (id and name), with sorting available by both attributes,

### prepare-bulk-config

Prepare config for insert-services-bulk.

After creation it should be edited to set correct service_types and physical_object_types.

### insert-services

Upload a single geojson as a list of services of given service type.

### insert-services-bulk

Same for the given directory of geojsons.

## Caution

1. At the current state, services insertion does not check if service is already exists, nor it checks availability of physical objects around the given geometry
2. Authentication is not yet supported
