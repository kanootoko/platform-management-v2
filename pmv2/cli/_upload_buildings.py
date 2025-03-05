"""Buildings uploading commands are defined here."""

import asyncio
import datetime
import json
import pickle
import sys
import time
from pathlib import Path
from typing import Any, Callable

import click
import geopandas as gpd

from pmv2.logic import upload_buildings as logic

from . import _mappers
from ._main import Config, main, pass_config


@main.group("buildings")
def buildings_group():
    """Operations with buildings."""


LIVING_BUILDING_NAME = "Жилой дом"
NON_LIVING_BUILDING_NAME = "Здание"
NON_LIVING_BUILDING_NAME = "Нежилое здание"


@buildings_group.command("upload-file")
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
    required=True,
    show_envvar=True,
    help="Field name to look at to check if the building is living",
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
    "--output-pickle",
    "-o",
    "output_file",
    type=click.Path(writable=True, path_type=Path),
    show_default="uploaded_one_<timestamp>.pickle",
    help="Output path for uploaded buildings data",
)
def upload_file(  # pylint: disable=too-many-locals
    config: Config,
    *,
    input_file: Path,
    is_living_field: str,
    parallel_workers: int,
    output_file: Path | None,
):
    """Upload a single geojson of buildings data.

    Be aware that it can place living buildings on top of non-living and otherwise.
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
    physical_object_types = asyncio.run(urban_client.get_physical_object_types())
    try:
        living_type_id = next(filter(lambda x: x.name == LIVING_BUILDING_NAME, physical_object_types))
        non_living_type_id = next(filter(lambda x: x.name == NON_LIVING_BUILDING_NAME, physical_object_types))
    except Exception:  # pylint: disable=broad-except
        logger.exception(
            "Error on getting living and non-living buildings physical objects types",
            living_name=LIVING_BUILDING_NAME,
            non_living_name=NON_LIVING_BUILDING_NAME,
        )
        sys.exit(1)
    physical_object_type_mapper = _get_physical_object_type_mapping_function(
        field_to_check=is_living_field,
        living_type_id=living_type_id.physical_object_type_id,
        non_living_type_id=non_living_type_id.physical_object_type_id,
    )
    results: dict[str, Any] = {
        "type": "upload_buildings",
        "time_start": datetime.datetime.now(),
        "input_file": str(input_file.resolve()),
        "config": {
            "is_living_field": is_living_field,
        },
    }
    gdf: gpd.GeoDataFrame = gpd.read_file(input_file)
    gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)

    def try_load_json(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        try:
            return json.loads(value)
        except Exception: # pylint: disable=broad-except
            return value
    for column in gdf.columns:
        gdf[column] = gdf[column].apply(try_load_json)

    po_uploader = logic.PhysicalObjectsUploader(
        urban_client,
        po_address_mapper=_mappers.get_attribute_mapper(["address"]),
        po_name_mapper=_mappers.get_func_mapper(
            ["name"],
            _mappers.get_string_checker_func(lambda name: f"(Здание {name})"),
            "(Безымянное здание)",
        ),
        po_properties_mapper=_mappers.full_dictionary_mapper,
        logger=config.logger,
    )
    uploader = logic.BuildingsUploader(
        urban_client,
        po_uploader=po_uploader,
        residents_number_mapper=_mappers.none_mapper,
        living_area_mapper=_mappers.get_attribute_mapper(["living_area"]),
        living_building_properties_mapper=_mappers.empty_dict_mapper,
        po_data_mapper=_mappers.full_dictionary_mapper,
        logger=logger,
    )
    try:
        uploaded, errors = asyncio.run(
            uploader.upload_buildings(
                gdf,
                physical_object_type_mapper=physical_object_type_mapper,
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
    with output_file.open("wb") as file:
        pickle.dump(results, file)


def _get_physical_object_type_mapping_function(
    field_to_check: str, living_type_id: int, non_living_type_id: int
) -> Callable[[dict[str, Any]], tuple[int, bool | None]]:
    def map_physical_object_type(properties: dict[str, Any]) -> int | None:
        if field_to_check not in properties or properties[field_to_check] is None:
            return non_living_type_id, None
        if properties[field_to_check] in (1, "1", True, "true"):
            return living_type_id, True
        return non_living_type_id, False

    return map_physical_object_type
