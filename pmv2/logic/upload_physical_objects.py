"""insert-services logic is defined here."""

import asyncio
import itertools
import math
from functools import partial
from typing import Any, Awaitable, Callable

import geopandas as gpd
import shapely
import structlog

from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import PostPhysicalObject, UrbanObject, shapely_to_geometry

logger: structlog.stdlib.BoundLogger = structlog.get_logger("upload_pysical_objects")


async def upload_physical_objects(  # pylint: disable=too-many-arguments
    urban_client: UrbanClient,
    gdf: gpd.GeoDataFrame,
    physical_object_type_id: int,
    parallel_workers: int = 1,
) -> list[UrbanObject | tuple[int, int]]:
    """Insert GeoDataFrame of physical objects of the same physical_object_type."""
    upload = partial(
        upload_physical_object_if_not_exists,
        urban_client=urban_client,
        physical_object_type_id=physical_object_type_id,
    )
    part_size = math.ceil(gdf.shape[0] / parallel_workers)
    gdfs = [gdf[i : i + part_size] for i in range(0, gdf.shape[0], part_size)]
    workers = [_upload_physical_objects(part, upload) for part in gdfs]
    inserted_services = list(itertools.chain.from_iterable(await asyncio.gather(*workers)))
    return inserted_services


async def _upload_physical_objects(
    gdf: gpd.GeoDataFrame,
    upload_func: Callable[..., Awaitable[UrbanObject | tuple[int, int] | tuple[None, None]]],
    max_errors: int | None = None,
) -> list[UrbanObject]:
    inserted_pos = []
    errors = 0
    for _, po_series in gdf.iterrows():
        try:
            po_data = po_series.dropna().to_dict()
            del po_data["geometry"]
            result = await upload_func(
                geometry=po_series["geometry"], name="(Unnamed physical object)", physical_object_data=po_data
            )
            if result == (None, None):
                logger.warning("Physical obejct has no territory parent. Skipping...", physical_object_data=po_data)
                continue
            inserted_pos.append(result)
        except Exception:  # pylint: disable=broad-except
            logger.exception("error on service insertion", physical_object_data=po_data)
            errors += 1
            if max_errors is not None and errors >= max_errors:
                logger.error("Finishing uploading worker because or errors rate", errors=errors)
                break
        except KeyboardInterrupt:
            await logger.awarning("Got interruption signal, finising")
            break
    return inserted_pos


async def upload_physical_object_if_not_exists(
    urban_client: UrbanClient,
    geometry: shapely.geometry.base.BaseGeometry,
    physical_object_type_id: int,
    physical_object_data: dict[str, Any],
    name: str,
) -> UrbanObject | tuple[int, int] | tuple[None, None]:
    """Check if there are suitable physical object and object geometry objects, create them if none found.

    Return full created urban object data or tuple of physical_object_id and object_geometry_id.

    Return tuple of None and None if it impossible to upload a physical object because of unavailable territory_id.
    """
    objects_around = await urban_client.get_objects_around(geometry, physical_object_type_id)
    if not geometry.is_valid:
        logger.warning("Invalid geometry in file, fixing", geometry=geometry)
        geometry = geometry.buffer(0)
    if not all(objects_around["geometry"].is_valid):
        logger.warning("Invalid geometry got from Urban API, fixing", around_geometry=geometry)
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
                    properties=physical_object_data,
                )
            )
            return result
        return None, None

    physical_object_id = intersecting.iloc[0]["physical_object_id"]

    geometries = await urban_client.get_physical_object_geometries(physical_object_id)
    geometries = geometries[
        geometries.intersects(geometry) | geometries.contains(geometry) | geometries.covered_by(geometry)
    ]
    geometry_id = geometries.iloc[0]["object_geometry_id"]
    return physical_object_id, geometry_id
