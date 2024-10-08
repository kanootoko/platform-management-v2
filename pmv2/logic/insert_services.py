"""insert-services logic is defined here."""

import asyncio
import math
from functools import partial
from typing import Any, Awaitable, Callable

import geopandas as gpd
import shapely
import structlog

from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import PostPhysicalObject, PostService, Service, shapely_to_geometry

logger: structlog.stdlib.BoundLogger = structlog.get_logger("insert_services")


async def insert_services(  # pylint: disable=too-many-arguments
    urban_client: UrbanClient,
    gdf: gpd.GeoDataFrame,
    service_type_id: int,
    physical_object_type_id: int,
    parallel_workers: int = 1,
) -> list[Service]:
    """Insert GeoDataFrame of services of the same service_type."""
    ensure = partial(
        ensure_physical_object_and_geometry,
        urban_client=urban_client,
        physical_object_type_id=physical_object_type_id,
    )
    upload = partial(insert_service, urban_client=urban_client, service_type_id=service_type_id)
    part_size = math.ceil(gdf.shape[0] / parallel_workers)
    gdfs = [gdf[i : i + part_size] for i in range(0, gdf.shape[0], part_size)]
    workers = [_insert_services(part, ensure, upload) for part in gdfs]
    inserted_services = await asyncio.gather(*workers)
    return inserted_services


async def _insert_services(
    gdf: gpd.GeoDataFrame,
    ensure_func: Callable[..., Awaitable[tuple[int, int]]],
    upload_func: Callable[..., Awaitable[Service]],
) -> list[Service]:
    inserted_services = []
    for i, service_series in gdf.iterrows():
        service_data = service_series.dropna().to_dict()
        del service_data["geometry"]
        physical_object_id, geometry_id = await ensure_func(
            geometry=service_series["geometry"], name=f"(Physical object for {_get_service_name(service_data)})"
        )
        if (physical_object_id, geometry_id) == (None, None):
            logger.warning("Service has no territory parent. Skipping...", iteration=i)
            continue
        try:
            inserted_services.append(
                await upload_func(
                    physical_object_id=physical_object_id, object_geometry_id=geometry_id, service_data=service_data
                )
            )
        except Exception:  # pylint: disable=broad-except
            logger.exception("error on service insertion")
    return inserted_services


async def ensure_physical_object_and_geometry(
    urban_client: UrbanClient,
    geometry: shapely.geometry.base.BaseGeometry,
    physical_object_type_id: int,
    name: str,
) -> tuple[int, int] | tuple[None, None]:
    """Check if there are suitable physical object and object geometry objects, create them if none found."""
    objects_around = await urban_client.get_objects_around(geometry, physical_object_type_id)
    if not geometry.is_valid:
        geometry = geometry.buffer(0)
        logger.warning("invalid geometry in file")
    if not all(objects_around["geometry"].is_valid):
        logger.warning("invalid geometry in gdf from UrbanApi")
        objects_around["geometry"] = objects_around["geometry"].buffer(0)
    intersecting = objects_around[
        objects_around.intersects(geometry) | objects_around.contains(geometry) | objects_around.covered_by(geometry)
    ]
    if intersecting.shape[0] == 0:
        territory_id = await urban_client.get_common_territory_id(geometry)
        if territory_id is not None:
            result = await urban_client.upload_physical_object(
                PostPhysicalObject(
                    geometry=shapely_to_geometry(geometry),
                    territory_id=territory_id,
                    physical_object_type_id=physical_object_type_id,
                    centre_point=None,
                    address=None,
                    name=name,
                    properties={},
                )
            )
            return result.physical_object.physical_object_id, result.object_geometry.object_geometry_id
        return None, None

    physical_object_id = intersecting.iloc[0]["physical_object_id"]

    geometries = await urban_client.get_physical_object_geometries(physical_object_id)
    geometries = geometries[
        geometries.intersects(geometry) | geometries.contains(geometry) | geometries.covered_by(geometry)
    ]
    geometry_id = geometries.iloc[0]["object_geometry_id"]
    return physical_object_id, geometry_id


async def insert_service(
    urban_client: UrbanClient,
    physical_object_id: int,
    object_geometry_id: int,
    service_data: dict[str, Any],
    service_type_id: int,
) -> Service:
    """Insert a single service to a given physical object and geometry."""
    return await urban_client.upload_service(
        PostService(
            physical_object_id=physical_object_id,
            object_geometry_id=object_geometry_id,
            service_type_id=service_type_id,
            territory_type_id=None,
            name=_get_service_name(service_data),
            capacity_real=_get_capacity(service_data, service_type_id, service_data),
            properties=service_data,
        )
    )


def _get_service_name(service_data: dict[str, Any]) -> str:
    for possible_name in ("name", "name:ru", "name:en", "description"):
        name = str(service_data.get(possible_name, ""))
        if len(name) > 0:
            return name
    return "(unnamed)"


capacity_dict: dict[int, int] = {}


def _get_capacity(service_data: dict[str, Any], service_type_id: int, properties: dict[str, Any]) -> int:
    for possible_name in ("capacity", "мощность"):
        try:
            capacity = int(service_data.get(possible_name, ""))
        except ValueError:
            continue
        return capacity
    properties["is_capacity_real"] = False
    return capacity_dict.get(service_type_id, 0)
