"""Physical objects uploading commands are defined here."""

import asyncio
import datetime
import pickle
import sys
import time
from pathlib import Path
from typing import Any

import click
import geopandas as gpd
import structlog
import yaml

from pmv2.logic import upload_physical_objects as logic
from pmv2.logic.upload_physical_objects_bulk import UploadConfig

from . import _mappers
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
    show_default="uploaded_one_<timestamp>.pickle",
    help="Output path for uploaded physical objects data",
)
def upload_file(
    config: Config,
    *,
    input_file: Path,
    physical_object_type_id: int,
    parallel_workers: int,
    output_file: Path | None,
):
    """Upload a single geojson of physical objects data."""
    if output_file is None:
        output_file = Path(f"uploaded_{int(time.time())}.pickle")
    if output_file.is_dir():
        output_file = output_file / f"uploaded_one_{int(time.time())}.pickle"
    urban_client = config.urban_client
    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    results: dict[str, Any] = {
        "type": "upload_physical_objects",
        "time_start": datetime.datetime.now(),
        "input_file": str(input_file.resolve()),
        "config": {
            "physical_object_type_id": physical_object_type_id,
        },
    }
    gdf: gpd.GeoDataFrame = gpd.read_file(input_file)
    gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)
    print(f"Read file {input_file.name} - {gdf.shape[0]} objects after filtering")
    uploader = logic.PhysicalObjectsUploader(
        urban_client,
        po_address_mapper=_mappers.none_mapper,
        po_name_mapper=_mappers.none_mapper,
        po_properties_mapper=_mappers.full_dictionary_mapper,
        logger=config.logger,
    )
    try:
        uploaded, errors = asyncio.run(uploader.upload_physical_objects(gdf, physical_object_type_id, parallel_workers))
    except KeyboardInterrupt:
        config.logger.error("Got interruption signal, impossible to save results")
        raise

    results["uploaded"] = [u.model_dump() for u in uploaded]
    results["errors"] = errors.to_geo_dict() if errors is not None else None
    results["metadata"] = {"total": gdf.shape[0], "uploaded": len(uploaded)}
    config.logger.info("Finished", log_filename=output_file.name)
    results["time_finish"] = datetime.datetime.now()
    with open(output_file, "wb") as file:
        pickle.dump(results, file)


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
    show_default="uploaded_<timestamp>.pickle",
    help="Output path for uploaded physical objects data",
)
def upload_bulk(  # pylint: disable=too-many-arguments,too-many-locals
    config: Config,
    *,
    input_dir: Path,
    upload_config_file: Path,
    parallel_workers: int,
    output_file: Path | None,
):
    """Execute a bulk upload of geojsons of physical objects data."""
    if output_file is None:
        output_file = Path(f"uploaded_{int(time.time())}.pickle")
    if not asyncio.run(config.urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)
    urban_client = config.urban_client
    logger = config.logger

    physical_object_types = asyncio.run(urban_client.get_physical_object_types())

    with upload_config_file.open(encoding="utf-8") as file:
        upload_config = UploadConfig.model_validate(yaml.safe_load(file)).transform_to_ids(physical_object_types)

    logger.info("Prepared upload config", config=upload_config)
    results: dict[str, Any] = {
        "type": "upload_physical_objects_bulk",
        "date": datetime.datetime.now(),
        "input_dir": str(input_dir.resolve()),
        "config": upload_config.model_dump(),
        "uploaded": {},
        "errors": {},
        "skipped": [],
        "metadata": {},
    }
    skipped = []
    for file in sorted(input_dir.glob("*.geojson")):
        if file.name not in upload_config.filenames:
            skipped.append(file.name)
            continue
        logger.info("Reading file", filename=file.name)
        gdf: gpd.GeoDataFrame = gpd.read_file(file)
        gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)
        physical_object_type_id = upload_config.filenames[file.name]
        structlog.contextvars.bind_contextvars(file=file.name)
        logger.info("Read file", objects=gdf.shape[0])
        if gdf.shape[0] == 0:
            logger.warning("Empty geojson file, skipping")
            continue
        uploader = logic.PhysicalObjectsUploader(
            urban_client,
            po_address_mapper=_mappers.get_attribute_mapper(["address"]),
            po_name_mapper=_mappers.get_attribute_mapper(["name"]),
            po_properties_mapper=_mappers.full_dictionary_mapper,
            logger=config.logger,
        )
        try:
            uploaded, errors = asyncio.run(
                uploader.upload_physical_objects(gdf, physical_object_type_id, parallel_workers)
            )
        except KeyboardInterrupt:
            logger.error("Got interruption signal, impossible to save part of results")
            break
        except Exception:  # pylint: disable=broad-except
            logger.exception("Got exception on processing file, ignoring")
            results["skipped"].append(file.name)
            continue

        results["uploaded"][file.name] = [s.model_dump() for s in uploaded]
        if errors is not None:
            results["errors"][file.name] = errors.to_geo_dict()
        results["metadata"][file.name] = {
            "total": gdf.shape[0],
            "uploaded": len(uploaded),
        }
    structlog.contextvars.unbind_contextvars("file")

    if len(skipped) > 0:
        logger.warning("Skipped some files", filenames=skipped)
    logger.info("Finished", log_filename=output_file.name)
    results["time_finish"] = datetime.datetime.now()
    with open(output_file, "wb") as file:
        pickle.dump(results, file)


@physical_objects_group.command("prepare-bulk-config")
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
    type=click.Path(dir_okay=False, path_type=Path),
    required=True,
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
