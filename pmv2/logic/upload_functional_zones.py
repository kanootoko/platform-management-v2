"""Functional zones upload logic is defined here."""

import asyncio
import datetime
import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import geopandas as gpd
import shapely
import structlog

from pmv2.logic.sqlite import SQLiteHelper
from pmv2.logic.utils import AlreadyLoggedException, logging_wrapper
from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import FunctionalZone, PostFunctionalZone, shapely_to_geometry


@dataclass
class FunctionalZoneForUpload:  # pylint: disable=too-many-instance-attributes
    """functional_zone prepared for upload from SQLite database."""

    id: int
    functional_zone_type_id: int
    year: int
    source: str
    name: str | None
    properties: dict[str, Any]
    geometry: shapely.geometry.base.BaseGeometry
    functional_zone_id: int | None


class FunctionalZonesUploader:
    """Functional zones uploader."""

    def __init__(
        self,
        urban_client: UrbanClient,
        *,
        sqlite: SQLiteHelper,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self._sqlite = sqlite
        if logger is ...:
            self._logger = structlog.get_logger("upload_functional_zones")
        else:
            self._logger = logger
        self._helper = FunctionalZonesHelper(sqlite, self._logger)
        self._helper.prepare_db()

    async def prepare_functional_zones(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        gdf: gpd.GeoDataFrame,
        *,
        filename: str,
        functional_zone_type_id_mapper: Callable[[dict[str, Any]], tuple[int, Callable[[dict[str, Any]], None]]],
        year_mapper: Callable[[dict[str, Any]], tuple[int, Callable[[dict[str, Any]], None]]],
        source_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        name_mapper: Callable[[dict[str, Any]], tuple[str | None, Callable[[dict[str, Any]], None]]],
        properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
    ) -> tuple[list[FunctionalZone], gpd.GeoDataFrame | None]:
        """Insert functional_zones in the SQLite database."""
        now = datetime.datetime.now()

        to_insert: list[dict[str, Any]] = []
        for _, po_series in gdf.iterrows():
            fz_data = po_series.dropna().to_dict()
            geometry: shapely.geometry.base.BaseGeometry = fz_data.pop("geometry")

            callbacks = []

            functional_zone_type_id, cb = functional_zone_type_id_mapper(fz_data)
            callbacks.append(cb)
            year, cb = year_mapper(fz_data)
            callbacks.append(cb)
            source, cb = source_mapper(fz_data)
            callbacks.append(cb)
            name, cb = name_mapper(fz_data)
            callbacks.append(cb)

            for cb in callbacks:
                cb(fz_data)

            properties, cb = properties_mapper(fz_data)
            cb(fz_data)

            to_insert.append(
                {
                    "functional_zone_type_id": functional_zone_type_id,
                    "year": year,
                    "source": source,
                    "name": name,
                    "properties": properties,
                    "geometry": geometry.wkt,
                    "added_at": now,
                    "filename": filename,
                }
            )

        inserted_ids = self._sqlite.insert_many(
            "functional_zones_data",
            data=to_insert,
            returning="id",
            columns=[
                "functional_zone_type_id",
                "year",
                "source",
                "name",
                "properties",
                "geometry",
                "added_at",
                "filename",
            ],
        )

        return inserted_ids

    async def upload_functional_zones(self, parallel_workers: int = 1) -> None:
        """Upload functional_zones from SQLite database in `parallel_workers` async workers."""
        total = self._helper.get_total()

        upload_func = logging_wrapper(
            self._logger, total, "preparing to upload functional_zone", self.upload_functional_zone_if_not_exists
        )
        workers = [self._uploading_worker_func(upload_func, worker_name=f"{i}") for i in range(parallel_workers)]

        await asyncio.gather(*workers)

    async def upload_functional_zone_if_not_exists(
        self, functional_zone: FunctionalZoneForUpload
    ) -> tuple[FunctionalZone | None, bool]:
        """Upload a single functional_zone of a given type checking for its existance.

        Return full created or found functional_zone data as first element of a tuple and True if it was uploaded now.

        Return None if it impossible to upload a functional_zone because of unavailable territory_id.
        """
        territory_id = await self._urban_client.get_common_territory_id(functional_zone.geometry)
        if territory_id is None:
            return None

        existing = await self._get_functional_zone(
            territory_id,
            functional_zone.geometry,
            functional_zone.functional_zone_type_id,
            functional_zone.year,
            functional_zone.source,
        )
        if existing is not None:
            self._logger.info(
                "return existing functional zone instead of uploading",
                territory_id=territory_id,
                year=functional_zone.year,
                source=functional_zone.source,
                functional_zone_type_id=functional_zone.functional_zone_type_id,
                properties=functional_zone.properties,
            )
            return existing, False

        functional_zone = PostFunctionalZone(
            geometry=shapely_to_geometry(functional_zone.geometry),
            territory_id=territory_id,
            functional_zone_type_id=functional_zone.functional_zone_type_id,
            name=functional_zone.name,
            year=functional_zone.year,
            source=functional_zone.source,
            properties=functional_zone.properties,
        )
        return await self._urban_client.upload_functional_zone(functional_zone), True

    async def _uploading_worker_func(
        self,
        upload_functional_zone: Callable[[FunctionalZoneForUpload], Awaitable[tuple[FunctionalZone, bool] | None]],
        worker_name: str | None = None,
    ) -> None:
        worker_logger = self._logger
        if worker_name is not None:
            worker_logger = worker_logger.bind(worker=worker_name)
        while True:
            try:
                functional_zone = self._helper.get_row_for_upload()
                if functional_zone is None:
                    worker_logger.info("no more objects to upload, finishing")
                    return
            except Exception as exc:  # pylint: disable=broad-except
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            try:
                result = await upload_functional_zone(functional_zone)
                if result is None:
                    self._helper.set_upload_error(functional_zone.id, "impossible to upload", non_retryable=True)
                    continue
                functional_zone_uploaded, new_uploaded = result
            except KeyboardInterrupt:
                worker_logger.info("finishing on interruption")
                return
            except Exception as exc:  # pylint: disable=broad-except
                self._helper.set_upload_error(functional_zone.id, repr(exc))
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            self._helper.set_upload_result(
                functional_zone.id, functional_zone_uploaded.functional_zone_id, not new_uploaded
            )

    async def _get_functional_zone(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        territory_id: int,
        geometry: shapely.geometry.base.BaseGeometry,
        functional_zone_type_id: int,
        year: int,
        source: str,
    ) -> FunctionalZone | None:
        existing = await self._urban_client.get_functional_zones(
            territory_id,
            year,
            source,
            functional_zone_type_id,
        )
        for zone in existing:
            zone_geometry = shapely.from_wkt(zone.geometry.wkt)
            intersection = zone_geometry.intersection(geometry)
            if intersection.area / geometry.area > 0.8 and intersection.area / zone_geometry.area > 0.8:
                return zone
        return None


class FunctionalZonesHelper:
    """Class which helps to upload functional_zones."""

    def __init__(self, sqlite: SQLiteHelper, logger: structlog.stdlib.BoundLogger):
        self._sqlite = sqlite
        self._logger = logger

    def prepare_db(self) -> None:
        "Prepare SQLite database."

        self._sqlite.execute(
            "CREATE TABLE IF NOT EXISTS functional_zones_data ("
            "   id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "   filename TEXT NOT NULL,"
            "   added_at TIMESTAMPTZ,"
            "   locked_till TIMESTAMPTZ,"
            "   functional_zone_type_id INTEGER NOT NULL,"
            "   year INTEGER NOT NULL,"
            "   source TEXT NOT NULL,"
            "   name TEXT,"
            "   properties TEXT,"
            "   geometry TEXT NOT NULL,"
            "   already_existed BOOLEAN,"
            "   error TEXT,"
            "   retry_count INTEGER DEFAULT 0,"
            "   functional_zone_id INTEGER"
            ")"
        )

    def get_row_for_upload(self) -> FunctionalZoneForUpload | None:
        """Select a functional_zone ready to be uploaded and set its locked_till 5 minutes to future."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "functional_zones_data",
            columns=["id", "functional_zone_type_id", "year", "source", "name", "properties", "geometry"],
            where=("functional_zone_id IS NULL" f" AND (locked_till IS NULL OR locked_till < '{now}')"),
            order_by="retry_count, added_at, locked_till",
            limit=1,
        )
        if len(results) == 0:
            return None
        result = results[0]
        self._sqlite.update(
            "functional_zones_data",
            where=f"id = {result['id']}",
            non_quoted_set="retry_count = retry_count + 1",
            locked_till=datetime.datetime.now() + datetime.timedelta(minutes=5),
        )
        try:
            if isinstance(result["properties"], str):
                result["properties"] = json.loads(result["properties"])
            result["geometry"] = shapely.wkt.loads(result["geometry"])
        except Exception as exc:
            self._logger.exception("Error on getting functional_zone from SQLite", id=result["id"])
            self.set_upload_error(result["id"], f"Error on get: {repr(exc)}")
            raise
        return FunctionalZoneForUpload(functional_zone_id=None, **result)

    def set_upload_result(self, functional_zone_id: int, external_id: int, already_existed: bool):
        """Set functional_zone id after uploading."""
        self._sqlite.update(
            "functional_zones_data",
            where=f"id = {functional_zone_id}",
            functional_zone_id=external_id,
            already_existed=already_existed,
        )

    def set_upload_error(self, functional_zone_id: int, error: str, non_retryable: bool = False):
        """Set error message for the given functional_zone. `non_retryable` flag will also move locked_till
        5 days to future.
        """
        to_update = {"error": error}
        if non_retryable:
            to_update["locked_till"] = datetime.datetime.now() + datetime.timedelta(days=10)
        self._sqlite.update("functional_zones_data", where=f"id = {functional_zone_id}", **to_update)

    def get_total(self) -> int:
        """Return total number of functional_zones ready to be uploaded at the moment."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "functional_zones_data",
            columns=["count(*)"],
            where=f"functional_zone_id IS NULL AND (locked_till IS NULL OR locked_till < '{now}')",
        )
        return results[0]["count(*)"]
