"""Services uploading commands are defined here."""

import asyncio
import datetime
import sqlite3
import sys
from pathlib import Path
from typing import Any

import click
import geopandas as gpd

from pmv2.logic.sqlite import SQLiteHelper
from pmv2.logic.upload_physical_objects import PhysicalObjectsUploader
from pmv2.logic.upload_services import ServicesUploader
from pmv2.logic.utils import read_geojson

from . import _mappers
from ._main import Config, main, pass_config


@main.group("services")
def services_group():
    """Operations with services."""


DEFAULT_NAME_ATTRIBUTES = ["name", "name:ru", "name:en", "description"]


@services_group.command("prepare-file")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to input geojson with services",
)
@click.option(
    "--service-type",
    "-s",
    type=str,
    required=True,
    help="Name/code of a service type",
)
@click.option(
    "--physical-object-type",
    "-p",
    type=str,
    required=True,
    help="Name/id of a physical_object type for a service",
)
@click.option(
    "--db-path",
    type=click.Path(writable=True, path_type=Path),
    default="db.sqlite",
    show_default=True,
    help="Path for SQLite database file for temporary data",
)
def upload_file(  # pylint: disable=too-many-locals
    config: Config,
    *,
    input_file: Path,
    service_type: str,
    physical_object_type: str,
    db_path: Path,
):
    """Prepare the upload of a single geojson of services data."""
    urban_client = config.urban_client
    logger = config.logger
    filename = str(input_file.resolve())

    metadata: dict[str, Any] = {
        "type": "prepare_services",
        "time_start": datetime.datetime.now(),
        "input_file": filename,
        "sqlite_database": str(db_path.resolve()),
        "config": {
            "service_type": service_type,
            "physical_object_type": physical_object_type,
        },
    }

    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    service_types = asyncio.run(urban_client.get_service_types())
    filtered = list(filter(lambda st: service_type in (st.name, st.code), service_types))
    if len(filtered) != 1:
        logger.error(
            "unable to set a service_type_id by name or code", service_type=service_type, filtered_servie_types=filtered
        )
        sys.exit(1)
    service_type_id = filtered[0].service_type_id

    po_types = asyncio.run(urban_client.get_physical_object_types())
    filtered = list(
        filter(
            lambda st: st.name == physical_object_type or str(st.physical_object_type_id) == physical_object_type,
            po_types,
        )
    )
    if len(filtered) != 1:
        logger.error(
            "unable to set a physical_object_type_id by name or id",
            physical_object_type=physical_object_type,
            filtered_servie_types=filtered,
        )
        sys.exit(1)
    physical_object_type_id = filtered[0].physical_object_type_id

    sqlite = SQLiteHelper(sqlite3.connect(db_path))

    logger.info("reading file", filename=filename)
    gdf: gpd.GeoDataFrame = read_geojson(input_file)
    logger.info("file is loaded", number_of_objects=gdf.shape[0])

    po_uploader = PhysicalObjectsUploader(
        urban_client,
        sqlite=sqlite,
        logger=logger,
    )
    uploader = ServicesUploader(
        urban_client,
        sqlite=sqlite,
        po_uploader=po_uploader,
        logger=logger,
    )

    ids = asyncio.run(
        uploader.prepare_services(
            gdf,
            filename=filename,
            service_type_id=service_type_id,
            physical_object_type_id=physical_object_type_id,
            service_name_mapper=_mappers.get_attribute_mapper(
                DEFAULT_NAME_ATTRIBUTES, f"({service_type} без названия)"
            ),
            service_properties_mapper=_mappers.full_dictionary_mapper,
            service_capacity_mapper=_mappers.get_service_capacity_mapper(None),
            po_osm_id_mapper=_mappers.get_attribute_mapper(["osmid", "osm_id", "id"]),
            po_address_mapper=_mappers.get_attribute_mapper(["address"]),
            po_name_mapper=_mappers.get_func_mapper(
                DEFAULT_NAME_ATTRIBUTES,
                _mappers.get_string_checker_func(lambda name: f"(Физический объект для сервиса {name})"),
                "(Безымянный физический объект)",
            ),
            po_data_mapper=_mappers.get_first_occurance_filter_dict_mapper(
                [DEFAULT_NAME_ATTRIBUTES, ["geometry"], ["osmid", "osm_id", "id"]]
            ),
            po_properties_mapper=_mappers.empty_dict_mapper,
        )
    )

    metadata["time_finish"] = datetime.datetime.now()
    metadata["number_of_objects"] = len(ids)
    logger.info("finished", metadata=metadata)


@services_group.command("upload")
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
    help="Number of workers to upload physical objects in parallel",
)
def upload(
    config: Config,
    *,
    db_path: Path,
    parallel_workers: int,
):
    """Upload services from SQLite database."""
    if not asyncio.run(config.urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    urban_client = config.urban_client
    logger = config.logger

    sqlite = SQLiteHelper(sqlite3.connect(db_path))

    po_uploader = PhysicalObjectsUploader(urban_client, sqlite=sqlite, logger=config.logger)
    uploader = ServicesUploader(urban_client, sqlite=sqlite, po_uploader=po_uploader, logger=logger)
    asyncio.run(uploader.upload_services(parallel_workers))
