"""Physical objects uploading commands are defined here."""

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
from pmv2.logic.utils import read_geojson

from . import _mappers
from ._main import Config, main, pass_config


@main.group("physical-objects")
def physical_objects_group():
    """Operations with physical objects."""


@physical_objects_group.command("prepare-file")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to input geojson with physical objects",
)
@click.option(
    "--physical-object-type",
    "-p",
    type=str,
    required=True,
    help="Name/id of a physical_object type",
)
@click.option(
    "--db-path",
    type=click.Path(writable=True, path_type=Path),
    default="db.sqlite",
    show_default=True,
    help="Path for SQLite database file for temporary data",
)
def prepare_file(
    config: Config,
    *,
    input_file: Path,
    physical_object_type: str,
    db_path: Path,
):
    """Prepare the upload of a single geojson of physical objects data."""
    urban_client = config.urban_client
    logger = config.logger
    filename = str(input_file.resolve())
    metadata: dict[str, Any] = {
        "type": "prepare_physical_objects",
        "time_start": datetime.datetime.now(),
        "input_file": filename,
        "sqlite_database": str(db_path.resolve()),
        "config": {
            "physical_object_type": physical_object_type,
        },
    }

    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

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

    uploader = PhysicalObjectsUploader(
        urban_client,
        sqlite=sqlite,
        logger=config.logger,
    )

    ids = asyncio.run(
        uploader.prepare_physical_objects(
            gdf,
            filename=filename,
            physical_object_type_id_mapper=_mappers.get_value_mapper(physical_object_type_id),
            address_mapper=_mappers.get_attribute_mapper(["address"]),
            osm_id_mapper=_mappers.get_attribute_mapper(["osmid", "osm_id", "id"]),
            name_mapper=_mappers.get_attribute_mapper(["name"]),
            properties_mapper=_mappers.full_dictionary_mapper,
        )
    )

    metadata["time_finish"] = datetime.datetime.now()
    metadata["number_of_objects"] = len(ids)
    logger.info("finished", metadata=metadata)


@physical_objects_group.command("upload")
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
    """Upload physical_objects from SQLite database."""
    if not asyncio.run(config.urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    urban_client = config.urban_client
    logger = config.logger

    sqlite = SQLiteHelper(sqlite3.connect(db_path))

    uploader = PhysicalObjectsUploader(urban_client, logger=logger, sqlite=sqlite)
    asyncio.run(uploader.upload_physical_objects(parallel_workers))
