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

Print territories hierarchy up to given level. Realization is really slow, there will be one API call
executed sequentially for each territory of given level limit (if set).

#### service-types, physical-object-types

Print list of correcponding entities (id and name), with sorting available by both attributes.

### physical-objects

Section to manipulate physical objects with geometries.

#### prepare-file

Store the given GeoJSON file with services data to the SQLite database for further upload process.
Multiple files can be stored in a single SQLite file. User can check database information to be sure what data will be uploaded exactly.

Attribute mappers are configured in code and may need some manual tuning.

Example:

```shell
pmv2 physical-objects prepare-file --db-path ./db.sqlite --physical-object-type Здание -i ./physical_objects.geojson
```

#### upload

Upload the physical objects from the prepared SQLite database to urban_api.
Physical_objects are checked for intersection with existing physical_objects on upload.
If an error happens, it is logged and saved to the SQLite database.

Example:

```shell
pmv2 physical-objects upload --db-path ./db.sqlite -w 10
```

### buildings

Section to manipulate buildings (which are physical_objects with optional building data).

Firstly designed to upload buildings from OSM + digital city platform + MS buildings project,
so mappers should be tuned when uploading from a single source.

#### prepare-file

Same as `physical-objects prepare-file`, but user can set `--is_living_field` option.

Example:

```shell
pmv2 buildings prepare-file -i ./builings.geojson --is-living-field living --db-path ./db.sqlite
```

#### upload

Same as `physical-objects upload`.

### services

#### prepare-file

Section to manipulate services.

Same as `physical-objects prepare-file`, but user must set `--service-type` (name/code of a service type)
and `--physical-object-type` (name/id of a physical_object type for cases when physical object is not found
and should be created too) options.

Example:

```shell
pmv2 services prepare-file -i "./school.geojson" -s Школа -p "Нежилое здание" --db-path ./db.sqlite
```

#### upload

Same as `physical-objects upload`.

### functional-zones

Section to manipulate functional_zones.

#### prepare-names-config

Create a yaml config with a mapping from names in the functional_zones geojson file to names of functional_zone
types in urban_api with default urban_api names as keys and values. User then should edit file to set keys as they
are in the geojson.

#### prepare-file

Same as `physical-objects prepare-file`, but user must set `--names-config` (path to yaml edited config),
`--year` and `--source` for the file, and can set `--functional-zone-type-field` to redefine attribute to check.

`--drop-unknown-fz-types` flag allows to upload only those functional_zones which can be mapped to urban_api. If it is
not set and such zone exists in a file, then script will fail.

Example:

```shell
pmv2 functional-zones prepare-file -i './functional_zones.geojson' --db-path ./db.sqlite --names-config ./fzt_names.yaml --year 2025 --source PZZ

```

#### upload

Same as `physical-objects upload`.

## Caution

1. Authentication is not yet supported
