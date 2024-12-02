"""Functional zones upload logic is defined here."""

import asyncio
import itertools
import math
from typing import Any, Awaitable, Callable

import geopandas as gpd
import pandas as pd
import shapely
import structlog

from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import FunctionalZone, PostFunctionalZone, shapely_to_geometry


class FunctionalZonesUploader:
    """Functional zones uploader."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        urban_client: UrbanClient,
        *,
        properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
        year_mapper: Callable[[dict[str, Any]], tuple[int, Callable[[dict[str, Any]], None]]],
        source_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        name_mapper: Callable[[dict[str, Any]], tuple[str | None, Callable[[dict[str, Any]], None]]],
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self._properties_mapper = properties_mapper
        self._year_mapper = year_mapper
        self._source_mapper = source_mapper
        self._name_mapper = name_mapper
        if logger is ...:
            self._logger = structlog.get_logger("upload_functional_zones")
        else:
            self._logger = logger

    async def upload_functional_zones(
        self,
        gdf: gpd.GeoDataFrame,
        functional_zone_type_mapper: Callable[[dict[str, Any]], int],
        parallel_workers: int = 1,
    ) -> tuple[list[FunctionalZone], gpd.GeoDataFrame | None]:
        """Upload GeoDataFrame of functional_zones with existing checking."""
        counter = 0

        def logging_wrapper(func: Awaitable[Callable[..., Any]]):
            async def wrapped(*args, **kwargs) -> Any:
                nonlocal counter
                counter += 1
                await self._logger.adebug("Preparing to upload functional zone", current=counter, total=gdf.shape[0])
                return await func(*args, **kwargs)

            return wrapped

        part_size = math.ceil(gdf.shape[0] / parallel_workers)
        gdfs = [gdf.iloc[i : i + part_size] for i in range(0, gdf.shape[0], part_size)]
        workers = [
            self._upload_functional_zones_batch(
                part, functional_zone_type_mapper, logging_wrapper(self.upload_functional_zone)
            )
            for part in gdfs
        ]

        results = await asyncio.gather(*workers)
        uploaded_functional_zones = list(itertools.chain.from_iterable(t[0] for t in results))
        error_gdfs = [t[1] for t in results if t[1] is not None]
        if len(error_gdfs) > 0:
            errors = pd.concat(error_gdfs)
        else:
            errors = None
        await self._logger.ainfo(
            "Finished functional_zones upload", total=gdf.shape[0], successful=len(uploaded_functional_zones)
        )
        return uploaded_functional_zones, errors

    async def upload_functional_zone(self, data: dict[str, Any], functional_zone_type_id: int) -> FunctionalZone | None:
        """Upload a single functional_zone of a given type."""
        geometry: shapely.geometry.base.BaseGeometry = data.pop("geometry")

        callbacks = []
        name, cb = self._name_mapper(data)
        callbacks.append(cb)
        year, cb = self._year_mapper(data)
        callbacks.append(cb)
        source, cb = self._source_mapper(data)
        callbacks.append(cb)

        for cb in callbacks:
            cb(data)

        properties, cb = self._properties_mapper(data)
        cb(data)

        territory_id = await self._urban_client.get_common_territory_id(geometry)
        if territory_id is None:
            return None

        existing = await self._get_functional_zone(territory_id, geometry, functional_zone_type_id)
        if existing is not None:
            self._logger.warning(
                "Return existing functional zone instead of uploading",
                territory_id=territory_id,
                functional_zone_type_id=functional_zone_type_id,
                properties=properties,
            )
            return existing

        functional_zone = PostFunctionalZone(
            geometry=shapely_to_geometry(geometry),
            territory_id=territory_id,
            functional_zone_type_id=functional_zone_type_id,
            name=name,
            year=year,
            source=source,
            properties=properties,
        )
        return await self._urban_client.upload_functional_zone(functional_zone)

    async def _get_functional_zone(
        self, territory_id: int, geometry: shapely.geometry.base.BaseGeometry, functional_zone_type_id: int
    ) -> FunctionalZone | None:
        existing = await self._urban_client.get_functional_zones(
            territory_id,
            functional_zone_type_id,
        )
        for zone in existing:
            zone_geometry = shapely.from_wkt(zone.geometry.wkt)
            intersection = zone_geometry.intersection(geometry)
            if intersection.area / geometry.area > 0.8 and intersection.area / zone_geometry.area > 0.8:
                return zone
        return None

    async def _upload_functional_zones_batch(
        self,
        gdf: gpd.GeoDataFrame,
        functional_zone_type_mapper: Callable[[dict[str, Any]], tuple[int, bool | None]],
        upload_functional_zone: Awaitable[Callable[[dict[str, Any], int], FunctionalZone | None]] = ...,
        max_errors: int | None = None,
    ) -> tuple[list[FunctionalZone], gpd.GeoDataFrame | None]:
        if upload_functional_zone is ...:
            upload_functional_zone = self.upload_functional_zone
        uploaded_functional_zones = []
        errors = []
        for idx, data_series in gdf.iterrows():
            try:
                full_data = data_series.dropna().to_dict()
                functional_zone_type_id = functional_zone_type_mapper(full_data)
                uploaded = await upload_functional_zone(data_series.dropna().to_dict(), functional_zone_type_id)
                if uploaded is None:
                    self._logger.warning("Functional zone has no territory parent. Skipping...", idx=idx)
                    errors.append(idx)
                else:
                    uploaded_functional_zones.append(uploaded)
            except Exception:  # pylint: disable=broad-except
                self._logger.exception("Error on functional zone upload", physical_object_data=full_data)
                errors.append(idx)
                if max_errors is not None and len(errors) >= max_errors:
                    self._logger.error("Finishing uploading worker because or errors rate", errors=len(errors))
                    break
            except KeyboardInterrupt:
                await self._logger.awarning("Got interruption signal, finising")
                break
        errors_gdf = gdf.loc[errors] if len(errors) > 0 else None
        return uploaded_functional_zones, errors_gdf
