"""Physical objects upload logic is defined here."""

import asyncio
import itertools
import math
from functools import partial
from typing import Any, Awaitable, Callable

import geopandas as gpd
import pandas as pd
import pyproj
import shapely
import shapely.ops
import structlog

from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import PostPhysicalObject, UrbanObject, shapely_to_geometry

_crs_transformer = pyproj.Transformer.from_crs(4326, 3857, always_xy=True)


class PhysicalObjectsUploader:
    """Physical objects uploader."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        urban_client: UrbanClient,
        *,
        po_address_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_name_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self._po_address_mapper = po_address_mapper
        self._po_name_mapper = po_name_mapper
        self._po_properties_mapper = po_properties_mapper
        if logger is ...:
            self._logger = structlog.get_logger("upload_pysical_objects")
        else:
            self._logger = logger

    async def upload_physical_objects(
        self,
        gdf: gpd.GeoDataFrame,
        physical_object_type_id: int,
        parallel_workers: int = 1,
    ) -> tuple[list[UrbanObject], gpd.GeoDataFrame | None]:
        """Upload GeoDataFrame of physical objects of the same physical_object_type.

        Return uploaded urban objects and GeoDataFrame with errors
        """
        counter = 0

        def logging_wrapper(func: Awaitable[Callable[..., Any]]):
            async def wrapped(*args, **kwargs):
                nonlocal counter
                counter += 1
                await self._logger.adebug("Preparing to upload physical object", current=counter, total=gdf.shape[0])
                return await func(*args, **kwargs)

            return wrapped

        upload_func = partial(
            logging_wrapper(self.upload_physical_object_if_not_exists),
            physical_object_type_id=physical_object_type_id,
        )
        part_size = math.ceil(gdf.shape[0] / parallel_workers)
        gdfs = [gdf.iloc[i : i + part_size] for i in range(0, gdf.shape[0], part_size)]
        workers = [self._upload_physical_objects_batch(part, upload_func) for part in gdfs]

        results = await asyncio.gather(*workers)
        uploaded_physical_objects = list(itertools.chain.from_iterable(t[0] for t in results))
        error_gdfs = [t[1] for t in results if t[1] is not None]
        if len(error_gdfs) > 0:
            errors = pd.concat(error_gdfs)
        else:
            errors = None
        await self._logger.ainfo(
            "Finished buildings uploading", total=gdf.shape[0], successful=len(uploaded_physical_objects)
        )
        return uploaded_physical_objects, errors

    async def _upload_physical_objects_batch(
        self,
        gdf: gpd.GeoDataFrame,
        upload_physical_object: Callable[..., Awaitable[UrbanObject | None]],
        max_errors: int | None = None,
    ) -> tuple[list[UrbanObject], gpd.GeoDataFrame | None]:
        uploaded_pos = []
        errors = []
        for idx, po_series in gdf.iterrows():
            try:
                po_data = po_series.dropna().to_dict()
                del po_data["geometry"]
                result = await upload_physical_object(geometry=po_series["geometry"], physical_object_data=po_data)
                if result is None:
                    self._logger.warning(
                        "Physical object has no territory parent. Skipping...", physical_object_data=po_data
                    )
                    errors.append(idx)
                else:
                    uploaded_pos.append(result)
            except Exception:  # pylint: disable=broad-except
                self._logger.exception("Error on physical object upload", physical_object_data=po_data)
                errors.append(idx)
                if max_errors is not None and len(errors) >= max_errors:
                    self._logger.error("Finishing uploading worker because or errors rate", errors=len(errors))
                    break
            except KeyboardInterrupt:
                await self._logger.awarning("Got interruption signal, finising")
                break
        errors_gdf = gdf.loc[errors] if len(errors) > 0 else None
        return uploaded_pos, errors_gdf

    async def upload_physical_object_if_not_exists(  # pylint: disable=too-many-locals
        self,
        geometry: shapely.geometry.base.BaseGeometry,
        physical_object_type_id: int,
        physical_object_data: dict[str, Any],
    ) -> UrbanObject | None:
        """Check if there are suitable physical object and object geometry objects, create them if none found.

        Return full created or found urban object data.

        Return None if it impossible to upload a physical object because of unavailable territory_id.
        """
        objects_around = await self._urban_client.get_objects_around(geometry, physical_object_type_id)
        if not geometry.is_valid:
            self._logger.warning("Invalid geometry in file, fixing", geometry=geometry)
            geometry = geometry.buffer(0)
        if not all(objects_around["geometry"].is_valid):
            self._logger.warning("Invalid geometry got from Urban API, fixing", around_geometry=geometry)
            objects_around["geometry"] = objects_around["geometry"].buffer(0)
        intersecting = self._get_intersecting_objects(geometry, objects_around)

        if intersecting.shape[0] == 0:
            territory_id = await self._urban_client.get_common_territory_id(geometry)
            if territory_id is None:
                return None

            callbacks = []

            address, cb = self._po_address_mapper(physical_object_data)
            callbacks.append(cb)
            name, cb = self._po_name_mapper(physical_object_data)
            callbacks.append(cb)
            properties, cb = self._po_properties_mapper(physical_object_data)
            callbacks.append(cb)

            for cb in callbacks:
                cb(physical_object_data)

            result = await self._urban_client.upload_physical_object(
                PostPhysicalObject(
                    geometry=shapely_to_geometry(geometry),
                    territory_id=territory_id,
                    physical_object_type_id=physical_object_type_id,
                    centre_point=None,
                    address=address,
                    name=name,
                    properties=properties,
                )
            )
            return result

        physical_object_id = intersecting.iloc[0]["physical_object_id"]

        geometries = await self._urban_client.get_physical_object_geometries(physical_object_id)
        geometries = self._get_intersecting_objects(geometry, geometries)
        geometry_id = geometries.iloc[0]["object_geometry_id"]
        return await self._urban_client.get_urban_object(physical_object_id, geometry_id, None)

    def _get_intersecting_objects(
        self,
        geometry: shapely.geometry.base.BaseGeometry,
        objects_around: gpd.GeoDataFrame,
        intersection_area_boundary: float = 0.6,
    ) -> gpd.GeoDataFrame:
        if objects_around.shape[0] == 0:
            return objects_around
        around_3857: gpd.GeoDataFrame = objects_around.to_crs(3857)
        around_3857["geometry"] = around_3857["geometry"].buffer(5)
        geometry_3857 = shapely.ops.transform(_crs_transformer.transform, geometry).buffer(5)
        around_3857 = around_3857[
            (
                around_3857.intersects(geometry_3857)
                | around_3857.contains(geometry_3857)
                | around_3857.covered_by(geometry_3857)
            )
        ]
        around_3857["intersection"] = around_3857.intersection(geometry_3857).area / geometry_3857.area

        intersecting = objects_around.loc[(around_3857["intersection"] > intersection_area_boundary).index].copy()
        intersecting["intersection"] = around_3857["intersection"]

        return intersecting.sort_values("intersection", ascending=False)
