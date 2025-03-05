"""Buildings upload logic is defined here."""

import asyncio
import itertools
import math
from typing import Any, Awaitable, Callable

import geopandas as gpd
import pandas as pd
import shapely
import structlog

from pmv2.logic.upload_physical_objects import PhysicalObjectsUploader
from pmv2.urban_client import UrbanClient
from pmv2.urban_client.exceptions import APIConnectionError, APITimeoutError
from pmv2.urban_client.http.exceptions import InvalidStatusCode
from pmv2.urban_client.models import UrbanObject


class BuildingsUploader:
    """Buildings uploader."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        urban_client: UrbanClient,
        *,
        po_uploader: PhysicalObjectsUploader,
        residents_number_mapper: Callable[[dict[str, Any]], tuple[int | None, Callable[[dict[str, Any]], None]]],
        living_area_mapper: Callable[[dict[str, Any]], tuple[float | None, Callable[[dict[str, Any]], None]]],
        living_building_properties_mapper: Callable[
            [dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]
        ],
        po_data_mapper: (
            Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]] | None
        ) = None,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self._po_uploader = po_uploader
        self._residents_number_mapper = residents_number_mapper
        self._living_area_mapper = living_area_mapper
        self._lb_properties_mapper = living_building_properties_mapper
        if po_data_mapper is None:
            self._po_data_mapper = lambda x: x
        else:
            self._po_data_mapper = po_data_mapper
        if logger is ...:
            self._logger = structlog.get_logger("upload_buildings")
        else:
            self._logger = logger

    async def upload_buildings(  # pylint: disable=too-many-arguments
        self,
        gdf: gpd.GeoDataFrame,
        physical_object_type_mapper: Callable[[dict[str, Any]], tuple[int, bool | None]],
        parallel_workers: int = 1,
    ) -> tuple[list[UrbanObject], gpd.GeoDataFrame | None]:
        """Upload GeoDataFrame of buildings with physical object type decided by mapper function."""
        counter = 0

        def logging_wrapper(func: Awaitable[Callable[..., Any]]):
            async def wrapped(*args, **kwargs) -> Any:
                nonlocal counter
                counter += 1
                await self._logger.adebug("Preparing to upload building", current=counter, total=gdf.shape[0])
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        return await func(*args, **kwargs)
                    except (APITimeoutError, InvalidStatusCode, APIConnectionError) as exc:
                        if isinstance(exc, InvalidStatusCode) and "504" not in str(exc):
                            raise
                        await self._logger.awarning(
                            "Suppressing urban_api error, sleeping for 5 seconds", error_type=type(exc), attempt=attempt
                        )
                        await asyncio.sleep(5)

            return wrapped

        part_size = math.ceil(gdf.shape[0] / parallel_workers)
        gdfs = [gdf.iloc[i : i + part_size] for i in range(0, gdf.shape[0], part_size)]
        workers = [
            self._upload_buildings_batch(part, physical_object_type_mapper, logging_wrapper(self.upload_building))
            for part in gdfs
        ]

        results = await asyncio.gather(*workers)
        uploaded_buildings = list(itertools.chain.from_iterable(t[0] for t in results))
        error_gdfs = [t[1] for t in results if t[1] is not None]
        if len(error_gdfs) > 0:
            errors = pd.concat(error_gdfs)
        else:
            errors = None
        await self._logger.ainfo("Finished buildings upload", total=gdf.shape[0], successful=len(uploaded_buildings))
        return uploaded_buildings, errors

    async def upload_building(self, full_data: dict[str, Any], physical_object_type_id: int, is_living: bool):
        """Upload a single building of a given physical_object_type and livinglesness."""
        full_data = full_data.copy()
        geometry: shapely.geometry.base.BaseGeometry = full_data.pop("geometry")
        callbacks = []

        resident_numer, cb = self._residents_number_mapper(full_data)
        callbacks.append(cb)
        living_area, cb = self._living_area_mapper(full_data)
        callbacks.append(cb)
        lb_properties, cb = self._lb_properties_mapper(full_data)
        callbacks.append(cb)
        physical_object_data, cb = self._po_data_mapper(full_data)
        callbacks.append(cb)

        result = await self._po_uploader.upload_physical_object_if_not_exists(
            geometry=geometry,
            physical_object_type_id=physical_object_type_id,
            physical_object_data=physical_object_data,
        )
        if result is None:
            self._logger.warning("Building has no territory parent. Skipping...", data=full_data)
            return None

        for cb in callbacks:
            cb(full_data)

        if is_living and result.physical_object.building is None:
            try:
                await self._urban_client.add_living_building(
                    result.physical_object.physical_object_id,
                    residents_number=resident_numer,
                    living_area=living_area,
                    properties=lb_properties,
                )
            except InvalidStatusCode as exc:
                if ": 409" not in str(exc):
                    raise
        return result

    async def _upload_buildings_batch(
        self,
        gdf: gpd.GeoDataFrame,
        physical_object_type_mapper: Callable[[dict[str, Any]], tuple[int, bool | None]],
        upload_building: Awaitable[Callable[[dict[str, Any], int, bool], UrbanObject | None]] = ...,
        max_errors: int | None = None,
    ) -> tuple[list[UrbanObject], gpd.GeoDataFrame | None]:
        if upload_building is ...:
            upload_building = self.upload_building
        uploaded_buildings = []
        errors = []
        for idx, data_series in gdf.iterrows():
            try:
                full_data = data_series.dropna().to_dict()
                physical_object_type_id, is_living = physical_object_type_mapper(full_data)
                uploaded = await upload_building(data_series.dropna().to_dict(), physical_object_type_id, is_living)
                if uploaded is not None:
                    uploaded_buildings.append(uploaded)
            except Exception:  # pylint: disable=broad-except
                self._logger.exception("Error on building upload", physical_object_data=full_data)
                errors.append(idx)
                if max_errors is not None and len(errors) >= max_errors:
                    self._logger.error("Finishing uploading worker because or errors rate", errors=len(errors))
                    break
            except KeyboardInterrupt:
                await self._logger.awarning("Got interruption signal, finising")
                break
        errors_gdf = gdf.loc[errors] if len(errors) > 0 else None
        return uploaded_buildings, errors_gdf
