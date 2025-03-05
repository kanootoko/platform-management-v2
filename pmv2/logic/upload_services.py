"""Services upload logic is defined here."""

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
from pmv2.urban_client.models import PostService, Service


class ServicesUploader:
    """Services uploader."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        urban_client: UrbanClient,
        *,
        po_uploader: PhysicalObjectsUploader,
        service_name_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        service_properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
        service_capacity_mapper: Callable[[dict[str, Any]], tuple[int, Callable[[dict[str, Any]], None]]],
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self._po_uploader = po_uploader
        self._service_name_mapper = service_name_mapper
        self._service_properties_mapper = service_properties_mapper
        self._service_capacity_mapper = service_capacity_mapper
        if logger is ...:
            self._logger = structlog.get_logger("upload_pysical_objects")
        else:
            self._logger = logger

    async def upload_services(  # pylint: disable=too-many-arguments
        self,
        gdf: gpd.GeoDataFrame,
        service_type_id: int,
        physical_object_type_id: int,
        parallel_workers: int = 1,
    ) -> tuple[list[Service], gpd.GeoDataFrame | None]:
        """Upload GeoDataFrame of services of the same service_type."""
        counter = 0

        def logging_wrapper(func: Awaitable[Callable[..., Any]]):
            async def wrapped(*args, **kwargs) -> Any:
                nonlocal counter
                counter += 1
                await self._logger.adebug("Preparing to upload service", current=counter, total=gdf.shape[0])
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
            self._upload_services_batch(
                part,
                physical_object_type_id=physical_object_type_id,
                service_type_id=service_type_id,
                upload_service=logging_wrapper(self.upload_service),
            )
            for part in gdfs
        ]

        results = await asyncio.gather(*workers)
        uploaded_services = list(itertools.chain.from_iterable(t[0] for t in results))
        error_gdfs = [t[1] for t in results if t[1] is not None]
        if len(error_gdfs) > 0:
            errors = pd.concat(error_gdfs)
        else:
            errors = None
        await self._logger.ainfo("Finished services uploading", total=gdf.shape[0], successful=len(uploaded_services))
        return uploaded_services, errors

    async def upload_service(
        self,
        service_data: dict[str, Any],
        physical_object_id: int,
        object_geometry_id: int,
        service_type_id: int,
    ) -> Service:
        """Upload a single service to a given physical object and geometry."""
        callbacks = []

        name, cb = self._service_name_mapper(service_data)
        callbacks.append(cb)
        capacity_real, cb = self._service_capacity_mapper(service_data)
        callbacks.append(cb)
        properties, cb = self._service_properties_mapper(service_data)
        callbacks.append(cb)

        for cb in callbacks:
            cb(service_data)
        return await self._urban_client.upload_service(
            PostService(
                physical_object_id=physical_object_id,
                object_geometry_id=object_geometry_id,
                service_type_id=service_type_id,
                territory_type_id=None,
                name=name,
                capacity_real=capacity_real,
                properties=properties,
            )
        )

    async def _upload_services_batch(  # pylint: disable=too-many-arguments
        self,
        gdf: gpd.GeoDataFrame,
        *,
        physical_object_type_id: int,
        service_type_id: int,
        upload_service: Awaitable[Callable[[dict[str, Any], int, int, int], Service]] = ...,
        max_errors: int | None = None,
    ) -> tuple[list[Service], gpd.GeoDataFrame | None]:
        uploaded_services = []
        if upload_service is ...:
            upload_service = self.upload_service
        errors = []
        for idx, service_series in gdf.iterrows():
            try:
                full_data = service_series.dropna().to_dict()
                geometry: shapely.geometry.base.BaseGeometry = full_data.pop("geometry")

                physical_object = await self._po_uploader.upload_physical_object_if_not_exists(
                    geometry=geometry,
                    physical_object_type_id=physical_object_type_id,
                    physical_object_data=full_data,
                )
                if physical_object is None:
                    await self._logger.awarning("Service has no territory parent. Skipping...", data=full_data)
                    errors.append(idx)
                    continue
                uploaded_services.append(
                    await upload_service(
                        physical_object_id=physical_object.physical_object.physical_object_id,
                        object_geometry_id=physical_object.object_geometry.object_geometry_id,
                        service_type_id=service_type_id,
                        service_data=full_data,
                    )
                )
            except Exception:  # pylint: disable=broad-except
                await self._logger.aexception("error on service upload", service_data=full_data)
                errors.append(idx)
                if max_errors is not None and len(errors) >= max_errors:
                    await self._logger.aerror("Finishing uploading worker because or errors rate", errors=len(errors))
                    break
        errors_gdf = gdf.loc[errors] if len(errors) > 0 else None
        return uploaded_services, errors_gdf
