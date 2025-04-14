"""Buildings uploading commands are defined here."""

import asyncio
import datetime
import sqlite3
import sys
from pathlib import Path
from typing import Any, Callable

import click
import geopandas as gpd

from pmv2.logic.sqlite import SQLiteHelper
from pmv2.logic.upload_buildings import BuildingsUploader
from pmv2.logic.upload_physical_objects import PhysicalObjectsUploader
from pmv2.logic.utils import read_geojson

from . import _mappers
from ._main import Config, main, pass_config


@main.group("buildings")
def buildings_group():
    """Operations with buildings."""


LIVING_BUILDING_NAME = "Жилой дом"
NON_LIVING_BUILDING_NAME = "Нежилое здание"


@buildings_group.command("prepare-file")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to input geojson with buildings",
)
@click.option(
    "--is-living-field",
    envvar="IS_LIVING_FIELD",
    type=str,
    default="is_living",
    show_envvar=True,
    show_default=True,
    help="Attribute name to look at to check if the building is living",
)
@click.option(
    "--db-path",
    type=click.Path(writable=True, path_type=Path),
    default="db.sqlite",
    show_default=True,
    help="Path for SQLite database file for temporary data",
)
def prepare_file(  # pylint: disable=too-many-locals
    config: Config,
    *,
    input_file: Path,
    is_living_field: str,
    db_path: Path,
):
    """Prepare a single geojson of buildings data for uploading."""
    urban_client = config.urban_client
    logger = config.logger
    filename = str(input_file.resolve())
    metadata: dict[str, Any] = {
        "type": "prepare_buildings",
        "time_start": datetime.datetime.now(),
        "input_file": str(input_file.resolve()),
        "sqlite_database": str(db_path.resolve()),
        "config": {
            "is_living_field": is_living_field,
        },
    }

    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    physical_object_types = asyncio.run(urban_client.get_physical_object_types())
    try:
        living_type_id = next(filter(lambda x: x.name == LIVING_BUILDING_NAME, physical_object_types))
        non_living_type_id = next(filter(lambda x: x.name == NON_LIVING_BUILDING_NAME, physical_object_types))
    except Exception:  # pylint: disable=broad-except
        logger.exception(
            "error on getting living and non-living buildings physical objects types",
            living_name=LIVING_BUILDING_NAME,
            non_living_name=NON_LIVING_BUILDING_NAME,
        )
        sys.exit(1)

    sqlite = SQLiteHelper(sqlite3.connect(db_path))

    physical_object_type_mapper = _get_physical_object_type_mapping_function(
        field_to_check=is_living_field,
        living_type_id=living_type_id.physical_object_type_id,
        non_living_type_id=non_living_type_id.physical_object_type_id,
    )

    logger.info("reading file", filename=filename)
    gdf: gpd.GeoDataFrame = read_geojson(input_file)
    logger.info("file is loaded", number_of_objects=gdf.shape[0])

    po_uploader = PhysicalObjectsUploader(
        urban_client,
        sqlite=sqlite,
        logger=logger,
    )
    uploader = BuildingsUploader(
        urban_client,
        sqlite=sqlite,
        po_uploader=po_uploader,
        logger=logger,
    )

    ids = asyncio.run(
        uploader.prepare_buildings(
            gdf,
            filename=filename,
            physical_object_type_mapper=physical_object_type_mapper,
            floors_mapper=_mappers.get_attribute_in_dicts_mapper(
                [["osm_data", "building:levels"], ["frt_data", "floor_count_max"], ["frt_data", "floor_count_min"]]
            ),
            building_area_official_mapper=_mappers.get_attribute_in_dicts_mapper([["frt_data", "area_land"]]),
            building_area_modeled_mapper=_mappers.none_mapper,
            project_type_mapper=_mappers.get_attribute_in_dicts_mapper([["frt_data", "project_type"]]),
            floor_type_mapper=_mappers.get_attribute_in_dicts_mapper([["frt_data", "floor_type"]]),
            wall_material_mapper=_mappers.get_attribute_in_dicts_mapper([["frt_data", "wall_material"]]),
            built_year_mapper=_mappers.get_attribute_in_dicts_mapper([["frt_data", "built_year"]]),
            exploitation_start_year_mapper=_mappers.get_attribute_in_dicts_mapper(
                [["frt_data", "exploitation_start_year"]]
            ),
            building_properties_mapper=_mappers.get_attribute_mapper(["frt_data"]),
            po_data_mapper=_mappers.get_dictionary_mapper_except_paths([["frt_data"], ["osm_data", "building:levels"]]),
            po_osm_id_mapper=_mappers.get_attribute_mapper(["osm_id"]),
            po_address_mapper=_mappers.get_osm_address_mapper("osm_data"),
            po_name_mapper=_mappers.get_func_mapper(
                ["name"],
                _mappers.get_string_checker_func(lambda name: f"(Здание {name})"),
                "(Безымянное здание)",
            ),
            po_properties_mapper=_mappers.full_dictionary_mapper,
        )
    )

    metadata["time_finish"] = datetime.datetime.now()
    metadata["number_of_objects"] = len(ids)
    logger.info("finished", metadata=metadata)


@buildings_group.command("upload")
@pass_config
@click.option(
    "--db-path",
    type=click.Path(exists=True, writable=True, path_type=Path),
    default="db.sqlite",
    show_default=True,
    help="Path for SQLite database file with temporary data",
)
@click.option(
    "--parallel-workers",
    "-w",
    type=int,
    default=1,
    show_default=True,
    help="Number of workers to upload buildings in parallel",
)
@click.option(
    "--skip-geometry-check",
    envvar="SKIP_GEOMETRY_CHECK",
    is_flag=True,
    show_envvar=True,
    help="Upload buildings geometry without checking for existance (can lead to duplicated data)",
)
def upload(config: Config, *, db_path: Path, parallel_workers: int, skip_geometry_check: bool):
    """Upload buildings from SQLite database.

    Be aware that it can place living buildings on top of non-living and otherwise.
    """
    if not asyncio.run(config.urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    urban_client = config.urban_client
    logger = config.logger

    sqlite = SQLiteHelper(sqlite3.connect(db_path))

    po_uploader = PhysicalObjectsUploader(
        urban_client, logger=logger, skip_geometry_check=skip_geometry_check, sqlite=sqlite
    )
    uploader = BuildingsUploader(urban_client, sqlite=sqlite, po_uploader=po_uploader, logger=logger)
    asyncio.run(uploader.upload_buildings(parallel_workers))


def _get_physical_object_type_mapping_function(
    field_to_check: str, living_type_id: int, non_living_type_id: int
) -> Callable[[dict[str, Any]], tuple[int, bool | None]]:
    def map_physical_object_type(properties: dict[str, Any]) -> int | None:
        if field_to_check not in properties or properties[field_to_check] is None:
            return non_living_type_id, None
        if properties[field_to_check] in (1, "1", True, "true"):
            del properties[field_to_check]
            return living_type_id, True
        del properties[field_to_check]
        return non_living_type_id, False

    return map_physical_object_type
