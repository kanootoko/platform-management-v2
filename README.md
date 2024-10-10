# platform-management-v2

This is a temporary utility for data manipulation of IDU Digital City Platform for the time until idu-uploader is fully functional.

Urban API can be obtained [here](https://github.com/iduprojects/idu_api) as a part of idu-api.

## Installation

1. Install Python3 and poetry
2. Clone this repo
3. Use `pip install .` for installation or `poetry install` and `poetry run pmv2 --help` to use in repository directory.

## Available commands

### list

Command group for getting lists of entities dictionaries available

#### territories

Print territories hierarchy up to given level. Realization is really slow, there will be one API call executed sequentially for each territory of given level limit (if set).

#### service-types, physical-object-types

Print list of correcponding entities (id and name), with sorting available by both attributes.

### services

### prepare-bulk-config

Prepare config for `upload-bulk`.

After creation it should be edited to set correct service_types and physical_object_types.

#### upload-file

Upload a single geojson as a list of services of given service type.

#### upload-bulk

Same for the given directory of geojsons with usage of edited config file.

### physical-objects

Similar to `services`, it allows to upload physical objects with geometry without service object.

## Caution

1. At the current state, services insertion does not check if service is already exists, nor it checks availability of physical objects around the given geometry
2. Geometry is checked only on intersection, without usage of buffering
3. Authentication is not yet supported
