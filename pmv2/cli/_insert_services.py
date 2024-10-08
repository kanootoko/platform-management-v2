"""Territories listing command is defined here."""

import asyncio
from pathlib import Path
import pickle
import time

import click
import geopandas as gpd
import structlog
import yaml

from pmv2.logic import insert_services as logic
from pmv2.logic.insert_services import capacity_dict
from pmv2.logic.insert_bulk import UploadConfig, UploadFileConfig
from pmv2.urban_client.models import Service

from ._main import Config, main, pass_config


@main.command("insert-services")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to input geojson with services",
)
@click.option(
    "--service-type-id",
    "-s",
    type=int,
    help="Indentifier of a service type",
)
@click.option(
    "--physical-object-type-id",
    "-p",
    type=int,
    help="Indentifier of a physical_object type",
)
@click.option(
    "--default-capacity",
    "-dc",
    type=int,
    help="Default capacity of service if not in data",
)
@click.option(
    "--parallel-workers",
    "-w",
    type=int,
    default=1,
    help="Number of workers to upload services in parallel",
)
@click.option(
    "--output-pickle",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    show_default="inserted_<timestamp>.pickle",
    help="Output path for inserted services data",
)
def insert_services(  # pylint: disable=too-many-arguments
    config: Config,
    input_file: Path,
    service_type_id: int,
    physical_object_type_id: int,
    default_capacity: int,
    parallel_workers: int,
    output_file: Path | None,
):
    """Upload a single geojson of services data.

    Do not check if service already exist. If no geometry is found, insert a new physical object of a given type
    """
    if output_file is None:
        output_file = Path(f"inserted_{int(time.time())}.pickle")
    urban_client = config.urban_client
    gdf: gpd.GeoDataFrame = gpd.read_file(input_file)
    gdf = gdf.drop_duplicates()
    gdf.to_crs(4326, inplace=True)
    print(f"Read file {input_file.name} - {gdf.shape[0]} objects after filtering")
    capacity_dict.update({service_type_id: default_capacity})
    inserted = asyncio.run(
        logic.insert_services(urban_client, gdf, service_type_id, physical_object_type_id, parallel_workers)
    )
    with open(output_file, "wb") as file:
        pickle.dump(inserted, file)


@main.command("insert-services-bulk")
@pass_config
@click.option(
    "--directory",
    "-d",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to input directory with services geojsons",
)
@click.option(
    "--config",
    "-c",
    "upload_config_file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="Path to bulk upload config yaml file",
)
@click.option(
    "--parallel-workers",
    "-w",
    type=int,
    default=1,
    show_default=True,
    help="Number of workers to upload services in parallel",
)
@click.option(
    "--output-pickle",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    show_default="inserted_<timestamp>.pickle",
    help="Output path for inserted services data",
)
def insert_services_bulk(  # pylint: disable=too-many-arguments
    config: Config,
    input_dir: Path,
    upload_config_file: Path,
    parallel_workers: int,
    output_file: Path | None,
):
    """Upload a batch of geojsons of services data.

    Do not check if service already exist. If no geometry is found, insert a new building.
    """
    if output_file is None:
        output_file = Path(f"inserted_{int(time.time())}.pickle")
    urban_client = config.urban_client
    logger = config.logger

    service_types = asyncio.run(urban_client.get_service_types())
    physical_object_types = asyncio.run(urban_client.get_physical_object_types())

    with upload_config_file.open(encoding="utf-8") as file:
        upload_config = UploadConfig.model_validate(yaml.safe_load(file)).transform_to_ids(
            service_types, physical_object_types
        )
    capacity_dict.update({data.service_type_id: data.default_capacity for data in upload_config.filenames.values()})
    logger.info("Prepared upload config", config=upload_config)
    results: dict[str, list[Service]] = {}
    skipped = []
    for file in sorted(input_dir.glob("*.geojson")):
        if file.name not in upload_config.filenames:
            skipped.append(file.name)
            continue
        logger.info("Reading file", filename=file.name)
        gdf: gpd.GeoDataFrame = gpd.read_file(file)
        gdf = gdf.drop_duplicates()
        gdf.to_crs(4326, inplace=True)
        service_type_id = upload_config.filenames[file.name].service_type_id
        physical_object_type_id = upload_config.filenames[file.name].physical_object_type_id
        logger.info("Read file", filename=file.name, objects=gdf.shape[0])
        structlog.contextvars.bind_contextvars(file=file.name)
        inserted = asyncio.run(
            logic.insert_services(urban_client, gdf, service_type_id, physical_object_type_id, parallel_workers)
        )
        results[file.name] = inserted
    structlog.contextvars.unbind_contextvars("file")

    if len(skipped) > 0:
        logger.warning("Skipped some files", filenames=skipped)
    logger.info("Finished")
    with open(output_file, "wb") as file:
        pickle.dump(results, file)


@main.command("prepare-bulk-config")
@click.option(
    "--directory",
    "-d",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Path to input directory with services geojsons",
)
@click.option(
    "--config",
    "-c",
    "upload_config_file",
    type=click.Path(dir_okay=False, path_type=Path),
    help="Path to save bulk config yaml file",
)
def prepare_bulk_config(  # pylint: disable=too-many-arguments
    input_dir: Path,
    upload_config_file: Path,
):
    """Upload a bulk of geojsons of services data.

    Do not check if service already exist. If no geometry is found, insert a new building.
    """
    config = UploadConfig(
        filenames={
            file.name: UploadFileConfig(service_type="___", physical_object_type="___", default_capacity=-1)
            for file in sorted(input_dir.glob("*.geojson"))
        }
    )
    with upload_config_file.open("w", encoding="utf-8") as file:
        yaml.dump(config.model_dump(), file)
