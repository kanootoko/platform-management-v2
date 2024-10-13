"""Territories listing command is defined here."""

import asyncio
import pickle
import sys
import time
from pathlib import Path

import click
import geopandas as gpd
import structlog
import yaml

from pmv2.logic import upload_physical_objects as logic
from pmv2.logic.upload_physical_objects_bulk import UploadConfig
from pmv2.urban_client.models import Service, UrbanObject

from ._main import Config, main, pass_config


@main.group("physical-objects")
def physical_objects_group():
    """Operations with physical objects."""


@physical_objects_group.command("upload-file")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to input geojson with physical objects",
)
@click.option(
    "--physical-object-type-id",
    "-p",
    type=int,
    required=True,
    help="Indentifier of a physical_object type",
)
@click.option(
    "--parallel-workers",
    "-w",
    type=int,
    default=1,
    show_default=True,
    help="Number of workers to upload physical objects in parallel",
)
@click.option(
    "--output-pickle",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    show_default="inserted_<timestamp>.pickle",
    help="Output path for inserted physical objects data",
)
def upload_file(  # pylint: disable=too-many-arguments
    config: Config,
    input_file: Path,
    physical_object_type_id: int,
    parallel_workers: int,
    output_file: Path | None,
):
    """Upload a single geojson of physical objects data.

    Do not check if physical objects already exist. If no geometry is found, insert a new physical object of
    a given type.
    """
    if output_file is None:
        output_file = Path(f"inserted_{int(time.time())}.pickle")
    if not asyncio.run(config.urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    urban_client = config.urban_client
    gdf: gpd.GeoDataFrame = gpd.read_file(input_file)
    gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)
    print(f"Read file {input_file.name} - {gdf.shape[0]} objects after filtering")
    try:
        inserted = asyncio.run(
            logic.upload_physical_objects(urban_client, gdf, physical_object_type_id, parallel_workers)
        )
    except KeyboardInterrupt:
        config.logger.error("Got interruption signal, impossible to save results")
        raise
    inserted = [
        s.model_dump() if isinstance(s, UrbanObject) else {"physical_object_id": s[0], "geometry_id": s[1]}
        for s in inserted
    ]

    with open(output_file, "wb") as file:
        pickle.dump(inserted, file)


@physical_objects_group.command("upload-bulk")
@pass_config
@click.option(
    "--directory",
    "-d",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Path to input directory with physical objects geojsons",
)
@click.option(
    "--config",
    "-c",
    "upload_config_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to bulk upload config yaml file",
)
@click.option(
    "--parallel-workers",
    "-w",
    type=int,
    default=1,
    show_default=True,
    help="Number of workers to upload physical objects in parallel",
)
@click.option(
    "--output-pickle",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    show_default="inserted_<timestamp>.pickle",
    help="Output path for inserted physical objects data",
)
def upload_bulk(  # pylint: disable=too-many-arguments,too-many-locals
    config: Config,
    input_dir: Path,
    upload_config_file: Path,
    parallel_workers: int,
    output_file: Path | None,
):
    """Execute a bulk upload of geojsons of physical objects data.

    Do not check if physical objects already exist. If no geometry is found, insert a new building.
    """
    if output_file is None:
        output_file = Path(f"inserted_{int(time.time())}.pickle")
    if not asyncio.run(config.urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    urban_client = config.urban_client
    logger = config.logger

    physical_object_types = asyncio.run(urban_client.get_physical_object_types())

    with upload_config_file.open(encoding="utf-8") as file:
        upload_config = UploadConfig.model_validate(yaml.safe_load(file)).transform_to_ids(physical_object_types)
    logger.info("Prepared upload config", config=upload_config)
    results: dict[str, list[Service]] = {}
    skipped = []
    for file in sorted(input_dir.glob("*.geojson")):
        if file.name not in upload_config.filenames:
            skipped.append(file.name)
            continue
        logger.info("Reading file", filename=file.name)
        gdf: gpd.GeoDataFrame = gpd.read_file(file)
        gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)
        physical_object_type_id = upload_config.filenames[file.name]
        logger.info("Read file", filename=file.name, objects=gdf.shape[0])
        structlog.contextvars.bind_contextvars(file=file.name)
        try:
            uploaded = asyncio.run(
                logic.upload_physical_objects(urban_client, gdf, physical_object_type_id, parallel_workers)
            )
        except KeyboardInterrupt:
            logger.error("Got interruption signal, impossible to save results")
            raise
        results[file.name] = [
            s.model_dump() if isinstance(s, UrbanObject) else {"physical_object_id": s[0], "geometry_id": s[1]}
            for s in uploaded
        ]
    structlog.contextvars.unbind_contextvars("file")

    if len(skipped) > 0:
        logger.warning("Skipped some files", filenames=skipped)
    logger.info("Finished")
    with open(output_file, "wb") as file:
        pickle.dump(results, file)


@physical_objects_group.command("prepare-bulk-config")
@click.option(
    "--directory",
    "-d",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to input directory with physical objects geojsons",
)
@click.option(
    "--config",
    "-c",
    "upload_config_file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Path to save bulk config yaml file",
)
def prepare_bulk_config(
    input_dir: Path,
    upload_config_file: Path,
):
    """Prepare a config for physical objects bulk upload based on geojsons in the given directory.

    User will need to fill service types (name attribute), default capacities different from -1
    (null is also acceptable) and physical object types of the physical objects.
    """
    config = UploadConfig(
        filenames={file.name: "(physical object type)" for file in sorted(input_dir.glob("*.geojson"))}
    )
    with upload_config_file.open("w", encoding="utf-8") as file:
        yaml.dump(config.model_dump(), file)
