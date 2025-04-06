"""Functional zones uploading commands are defined here."""

import asyncio
import datetime
import math
import sqlite3
import sys
from pathlib import Path
from typing import Any

import click
import geopandas as gpd
import structlog
import yaml

from pmv2.cli import _mappers
from pmv2.logic.sqlite import SQLiteHelper
from pmv2.logic.upload_functional_zones import FunctionalZonesUploader
from pmv2.logic.utils import read_geojson

from ._main import Config, main, pass_config


@main.group("functional-zones")
def functional_zones_group():
    """Operations with functional zones."""


@functional_zones_group.command("prepare-file")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to input geojson with functional zones",
)
@click.option(
    "--names-config",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to yaml config to map functional zone types",
)
@click.option(
    "--year",
    "-y",
    type=int,
    required=True,
    help="Year of functional zone",
)
@click.option(
    "--source",
    "-s",
    type=str,
    required=True,
    help="Source of a functional zone",
)
@click.option(
    "--functional-zone-type-field",
    type=str,
    default="landuse_zon",
    envvar="FUNCTIONAL_ZONE_TYPE_FIELD",
    show_default=True,
    show_envvar=True,
    help="Source of a functional zone attribute",
)
@click.option(
    "--db-path",
    type=click.Path(writable=True, path_type=Path),
    default="db.sqlite",
    show_default=True,
    help="Path for SQLite database file for temporary data",
)
@click.option(
    "--drop-unknown-fz-types", is_flag=True, help="Drop unknown functional_zone types instead of aborting preparations"
)
def upload_file(  # pylint: disable=too-many-arguments,too-many-locals
    config: Config,
    *,
    names_config: Path,
    input_file: Path,
    year: int,
    source: str,
    functional_zone_type_field: str,
    db_path: Path,
    drop_unknown_fz_types: bool,
):
    """Prepare the upload of a single geojson of functional_zones data."""
    urban_client = config.urban_client
    logger = config.logger
    filename = str(input_file.resolve())
    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

    with names_config.open("r", encoding="utf-8") as file:
        fzt_names_mapping: dict[str, str] = yaml.safe_load(file)

    functional_zone_types = asyncio.run(urban_client.get_functional_zone_types())
    actual_fz_types = {fzt.name: fzt.functional_zone_type_id for fzt in functional_zone_types}

    metadata: dict[str, Any] = {
        "type": "upload_functional_zones",
        "time_start": datetime.datetime.now(),
        "input_file": filename,
        "sqlite_database": str(db_path.resolve()),
        "config": {
            "year": year,
            "source": source,
        },
    }

    sqlite = SQLiteHelper(sqlite3.connect(db_path))

    logger.info("reading file", filename=filename)
    gdf: gpd.GeoDataFrame = read_geojson(input_file)
    logger.info("file is loaded", number_of_objects=gdf.shape[0])

    gdf = _check_unknown_fz_types(
        gdf,
        functional_zone_type_field=functional_zone_type_field,
        fzt_names_mapping=fzt_names_mapping,
        actual_fz_types=actual_fz_types,
        drop_unknown_fz_types=drop_unknown_fz_types,
        logger=logger,
    )

    uploader = FunctionalZonesUploader(
        urban_client,
        sqlite=sqlite,
        logger=config.logger,
    )
    ids = asyncio.run(
        uploader.prepare_functional_zones(
            gdf,
            filename=filename,
            functional_zone_type_id_mapper=_mappers.get_attribute_mapper([functional_zone_type_field], None),
            year_mapper=_mappers.get_value_mapper(year),
            source_mapper=_mappers.get_value_mapper(source),
            name_mapper=_mappers.get_attribute_mapper(["name"]),
            properties_mapper=_mappers.full_dictionary_mapper,
        )
    )

    metadata["time_finish"] = datetime.datetime.now()
    metadata["number_of_objects"] = len(ids)
    logger.info("finished", metadata=metadata)


@functional_zones_group.command("upload")
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
    help="Number of workers to upload functional zones in parallel",
)
def upload(
    config: Config,
    *,
    db_path: Path,
    parallel_workers: int,
):
    """Upload functional_zones from SQLite database."""
    if not asyncio.run(config.urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    urban_client = config.urban_client
    logger = config.logger

    sqlite = SQLiteHelper(sqlite3.connect(db_path))

    uploader = FunctionalZonesUploader(urban_client, logger=logger, sqlite=sqlite)
    asyncio.run(uploader.upload_functional_zones(parallel_workers))


@functional_zones_group.command("prepare-names-config")
@pass_config
@click.option(
    "--config",
    "names_config",
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
    help="Path to yaml config to map functional zone types",
)
def prepare_names_config(config: Config, names_config: Path):
    """Get functional zone types mapper config template."""
    urban_client = config.urban_client

    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

    functional_zone_types = asyncio.run(urban_client.get_functional_zone_types())
    fz_types_names = {fzt.name for fzt in functional_zone_types}

    with names_config.open("w", encoding="utf-8") as file:
        yaml.safe_dump({fzt: fzt for fzt in fz_types_names}, file)


def _check_unknown_fz_types(  # pylint: disable=too-many-arguments
    gdf: gpd.GeoDataFrame,
    *,
    functional_zone_type_field: str,
    fzt_names_mapping: dict[str, str],
    actual_fz_types: dict[str, int],
    drop_unknown_fz_types: bool,
    logger: structlog.stdlib.BoundLogger,
) -> gpd.GeoDataFrame:
    """Check that functional_zone_type column is present, and all of its values mapped through
    `fzt_names_mapping` (name in file -> name in urban_api) and `actual_fz_types` (name in urban_api -> id in urban_api)
    are valid.

    If not all of the possible values are present, exit(1) or filter depending on `drop_unknown_fz_types` flag.
    """
    if functional_zone_type_field not in gdf.columns:
        logger.error(
            "input gdf is missing functional_zone_type field", functional_zone_type_field=functional_zone_type_field
        )
        sys.exit(1)

    gdf[functional_zone_type_field] = gdf[functional_zone_type_field].map(lambda fzt: fzt_names_mapping.get(fzt, fzt))

    fzt_in_gdf = set(gdf[functional_zone_type_field].unique())
    unknown_fz_types = fzt_in_gdf - set(actual_fz_types)

    if len(unknown_fz_types) > 0 and not drop_unknown_fz_types:
        logger.error(
            "some functional_zone_type values cannot be mapped", unknowns=sorted(fzt_in_gdf - set(actual_fz_types))
        )
        sys.exit(1)

    gdf[functional_zone_type_field] = gdf[functional_zone_type_field].map(
        lambda fzt: actual_fz_types.get(fzt, math.nan)
    )

    if len(unknown_fz_types) == 0:
        return gdf

    gdf.dropna(subset=functional_zone_type_field, inplace=True)

    logger.info("filtered gdf on functional_zone_type values", new_number_of_objects=gdf.shape[0])

    return gdf
