"""Services uploading commands are defined here."""

import asyncio
import datetime
import pickle
import sys
import time
from pathlib import Path
from typing import Any, Callable

import click
import geopandas as gpd
import structlog
import yaml

from pmv2.cli import _mappers
from pmv2.logic.upload_functional_zones import FunctionalZonesUploader

from ._main import Config, main, pass_config


@main.group("functional-zones")
def functional_zones_group():
    """Operations with functional zones."""


DEFAULT_SERVICE_NAME = "(Сервис без названия)"
DEFAULT_NAME_ATTRIBUTES = ["name", "name:ru", "name:en", "description"]


@functional_zones_group.command("upload-file")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to input geojson with services",
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
    help="Source of a functional zone",
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
    show_default="uploaded_one_<timestamp>.pickle",
    help="Output path for uploaded services data",
)
def upload_file(  # pylint: disable=too-many-arguments,too-many-locals
    config: Config,
    *,
    names_config: Path,
    input_file: Path,
    year: int,
    source: str,
    parallel_workers: int,
    functional_zone_type_field: str,
    output_file: Path | None,
):
    """Upload a single geojson of services data.

    Do not check if service already exist. If no geometry is found, upload a new physical object of a given type.
    """
    if output_file is None:
        output_file = Path(f"uploaded_one_{int(time.time())}.pickle")
    if output_file.is_dir():
        output_file = output_file / f"uploaded_one_{int(time.time())}.pickle"
    urban_client = config.urban_client
    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

    with names_config.open("r", encoding="utf-8") as file:
        fzt_names_mapping = yaml.safe_load(file)

    def map_fzt_name(s: Any) -> str:
        if s in fzt_names_mapping:
            return fzt_names_mapping[s]
        return str(s)

    functional_zone_types = asyncio.run(urban_client.get_functional_zone_types())
    fz_types = {fzt.name: fzt.functional_zone_type_id for fzt in functional_zone_types}

    results: dict[str, Any] = {
        "type": "upload_functional_zones",
        "time_start": datetime.datetime.now(),
        "input_file": str(input_file.resolve()),
        "config": {
            "year": year,
            "source": source,
        },
    }

    gdf: gpd.GeoDataFrame = gpd.read_file(input_file)
    gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)
    print(f"Read file {input_file.name} - {gdf.shape[0]} objects after filtering")

    if functional_zone_type_field not in gdf.columns:
        print(f"Missing functional_zone_type field: '{functional_zone_type_field}'")
        sys.exit(1)
    fzt_file = set(map(map_fzt_name, gdf[functional_zone_type_field]))
    if len(fzt_file - set(fz_types)) > 0:
        print("Following functional_zone_type values cannot be mapped:", ", ".join(sorted(fzt_file - set(fz_types))))
        sys.exit(1)

    uploader = FunctionalZonesUploader(
        urban_client,
        properties_mapper=_mappers.full_dictionary_mapper,
        year_mapper=_mappers.get_value_mapper(year),
        source_mapper=_mappers.get_value_mapper(source),
        name_mapper=_mappers.get_attribute_mapper(["name"]),
        logger=config.logger,
    )
    try:
        uploaded, errors = asyncio.run(
            uploader.upload_functional_zones(
                gdf,
                functional_zone_type_mapper=lambda d: fz_types[map_fzt_name(d.pop(functional_zone_type_field, None))],
                parallel_workers=parallel_workers,
            )
        )
    except KeyboardInterrupt:
        config.logger.error("Got interruption signal, impossible to save results")
        sys.exit(1)

    results["uploaded"] = [u.model_dump() for u in uploaded]
    results["errors"] = errors.to_geo_dict() if errors is not None else None
    results["metadata"] = {"total": gdf.shape[0], "uploaded": len(uploaded)}
    config.logger.info("Finished", log_filename=output_file.name)
    results["time_finish"] = datetime.datetime.now()
    with open(output_file, "wb") as file:
        pickle.dump(results, file)


@functional_zones_group.command("upload-bulk")
@pass_config
@click.option(
    "--directory",
    "-d",
    "input_dir",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    required=True,
    help="Path to input geojson with services",
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
    help="Source of a functional zone",
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
    show_default="uploaded_one_<timestamp>.pickle",
    help="Output path for uploaded services data",
)
def upload_bulk(  # pylint: disable=too-many-arguments,too-many-locals
    config: Config,
    *,
    names_config: Path,
    input_dir: Path,
    year: int,
    source: str,
    parallel_workers: int,
    functional_zone_type_field: str,
    output_file: Path | None,
):
    """Upload a single geojson of services data.

    Do not check if service already exist. If no geometry is found, upload a new physical object of a given type.
    """
    if output_file is None:
        output_file = Path(f"uploaded_{int(time.time())}.pickle")
    if output_file.is_dir():
        output_file = output_file / f"uploaded_one_{int(time.time())}.pickle"
    urban_client = config.urban_client
    logger = config.logger
    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

    with names_config.open("r", encoding="utf-8") as file:
        fzt_names_mapping = yaml.safe_load(file)

    def map_fzt_name(s: Any) -> str:
        if s in fzt_names_mapping:
            return fzt_names_mapping[s]
        return str(s)

    functional_zone_types = asyncio.run(urban_client.get_functional_zone_types())
    fz_types = {fzt.name: fzt.functional_zone_type_id for fzt in functional_zone_types}

    results: dict[str, Any] = {
        "type": "upload_functional_zones_bulk",
        "time_start": datetime.datetime.now(),
        "input_dir": str(input_dir.resolve()),
        "config": {
            "year": year,
            "source": source,
        },
        "uploaded": {},
        "errors": {},
        "skipped": [],
        "metadata": {},
    }

    uploader = FunctionalZonesUploader(
        urban_client,
        properties_mapper=_mappers.full_dictionary_mapper,
        year_mapper=_mappers.get_value_mapper(year),
        source_mapper=_mappers.get_value_mapper(source),
        name_mapper=_mappers.get_attribute_mapper(["name"]),
        logger=logger,
    )

    for file in sorted(input_dir.glob("*.geojson")):
        structlog.contextvars.bind_contextvars(file=file.name)
        logger.info("Reading file")
        gdf: gpd.GeoDataFrame = gpd.read_file(file)
        gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)
        print(f"Read file {file.name} - {gdf.shape[0]} objects after filtering")

        if functional_zone_type_field not in gdf.columns:
            print(f"Missing functional_zone_type field: '{functional_zone_type_field}'")
            sys.exit(1)
        fzt_file = set(map(map_fzt_name, gdf[functional_zone_type_field]))
        if len(fzt_file - set(fz_types)) > 0:
            logger.error(
                "Some functional_zone_type values cannot be mapped skipping file",
                functional_zones=sorted(fzt_file - set(fz_types)),
            )
            results["skipped"].append(file.name)
            continue

        try:
            uploaded, errors = asyncio.run(
                uploader.upload_functional_zones(
                    gdf,
                    functional_zone_type_mapper=lambda d: fz_types[
                        map_fzt_name(d.pop(functional_zone_type_field, None))
                    ],
                    parallel_workers=parallel_workers,
                )
            )
        except KeyboardInterrupt:
            logger.error("Got interruption signal, impossible to save part of results")
            break
        except Exception:  # pylint: disable=broad-except
            results["skipped"].append(file.name)
            logger.exception("Got exception on processing file, ignoring")
            continue

        results["uploaded"][file.name] = [u.model_dump() for u in uploaded]
        if errors is not None:
            results["errors"][file.name] = errors.to_geo_dict()
        results["metadata"][file.name] = {"total": gdf.shape[0], "uploaded": len(uploaded)}
    structlog.contextvars.unbind_contextvars("file")

    logger.info("Finished", log_filename=output_file.name)
    results["time_finish"] = datetime.datetime.now()
    with open(output_file, "wb") as file:
        pickle.dump(results, file)


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


def _get_additionals_properties_mapper(
    additionals: dict[str, Any]
) -> Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]]:
    def mapper(data: dict[str, Any]) -> dict[str, Any]:
        result = data.copy()
        result.update(additionals)
        return result, lambda _: None

    return mapper
