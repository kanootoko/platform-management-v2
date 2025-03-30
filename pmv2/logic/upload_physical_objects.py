"""Physical_objects upload logic is defined here."""

import asyncio
import datetime
import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import shapely.ops
import shapely.wkt
import structlog

from pmv2.logic.sqlite import SQLiteHelper
from pmv2.logic.utils import AlreadyLoggedException, logging_wrapper, transform_geometry_4326_to_3857
from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import PostPhysicalObject, UrbanObject, shapely_to_geometry


@dataclass
class PhysicalObjectForUpload:  # pylint: disable=too-many-instance-attributes
    """physical_object prepared for upload from SQLite database."""

    id: int
    physical_object_type_id: int
    address: str
    name: str
    properties: dict[str, Any]
    geometry: shapely.geometry.base.BaseGeometry
    physical_object_id: int | None
    geometry_id: int | None


class ImpossibleToUploadPhysicalObjectError(RuntimeError):
    """Physical object geometry does not have a valid territory underneath."""


class PhysicalObjectsUploader:
    """Physical_objects uploader."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        urban_client: UrbanClient,
        *,
        sqlite: SQLiteHelper,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self._sqlite = sqlite
        if logger is ...:
            self._logger = structlog.get_logger("upload_pysical_objects")
        else:
            self._logger = logger
        self._helper = PhysicalObjectsHelper(sqlite, self._logger)
        self._helper.prepare_db()

    async def prepare_physical_objects(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        gdf: gpd.GeoDataFrame,
        *,
        filename: str,
        physical_object_type_id_mapper: Callable[[dict[str, Any]], tuple[int, Callable[[dict[str, Any]], None]]],
        address_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        name_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
    ) -> list[int]:
        """Insert physical_objects in the SQLite database."""
        now = datetime.datetime.now()

        to_insert: list[dict[str, Any]] = []
        for _, po_series in gdf.iterrows():
            po_data = po_series.dropna().to_dict()
            geometry: shapely.geometry.base.BaseGeometry = po_data.pop("geometry")

            callbacks = []

            address, cb = address_mapper(po_data)
            callbacks.append(cb)
            name, cb = name_mapper(po_data)
            callbacks.append(cb)
            physical_object_type_id, cb = physical_object_type_id_mapper(po_data)
            callbacks.append(cb)

            for cb in callbacks:
                cb(po_data)
            properties, cb = properties_mapper(po_data)
            cb(po_data)

            to_insert.append(
                {
                    "address": address,
                    "name": name,
                    "properties": properties,
                    "geometry": geometry.wkt,
                    "physical_object_type_id": physical_object_type_id,
                    "added_at": now,
                    "filename": filename,
                }
            )

        inserted_ids = self._sqlite.insert_many(
            "physical_objects_data",
            data=to_insert,
            returning="id",
            columns=["address", "name", "properties", "geometry", "physical_object_type_id", "added_at", "filename"],
        )

        return inserted_ids

    async def upload_physical_objects(self, parallel_workers: int = 1) -> None:
        """Upload physical_objects from SQLite database in `parallel_workers` async workers."""
        total = self._helper.get_total()

        upload_func = logging_wrapper(
            self._logger, total, "preparing to upload physical_object", self.upload_physical_object_if_not_exists
        )
        workers = [self._uploading_worker_func(upload_func, worker_name=f"{i}") for i in range(parallel_workers)]

        await asyncio.gather(*workers)

    async def upload_one_if_not_exists(self, physical_object_id: int) -> tuple[UrbanObject, bool] | None:
        """Upload a physical_object after checking that it does not exist - or return existing one."""
        physical_object = self._helper.get_row_by_id(physical_object_id)
        if physical_object.geometry_id is not None and physical_object.physical_object_id is not None:
            return await self._urban_client.get_urban_object_by_composite(
                physical_object.physical_object_id, physical_object.geometry_id, None
            )
        try:
            result = await self.upload_physical_object_if_not_exists(physical_object)
            if result is None:
                self._helper.set_upload_error(physical_object.id, "impossible to upload", non_retryable=True)
                raise ImpossibleToUploadPhysicalObjectError()
            urban_object, new_uploaded = result
        except Exception as exc:
            self._helper.set_upload_error(physical_object.id, repr(exc))
            raise
        self._helper.set_upload_result(
            physical_object.id,
            urban_object.physical_object.physical_object_id,
            urban_object.object_geometry.object_geometry_id,
            not new_uploaded,
        )
        return result

    async def upload_physical_object_if_not_exists(
        self, physical_object: PhysicalObjectForUpload
    ) -> tuple[UrbanObject, bool] | None:
        """Check if there are suitable physical_object and object_geometry objects, create them if none found.

        Return full created or found urban_object data as first element of a tuple and True if a new physical_object
        was uploaded instead of finding an existing one.

        Return None if it impossible to upload a physical_object because of unavailable territory_id.
        """
        objects_around = await self._urban_client.get_objects_around(
            physical_object.geometry, physical_object.physical_object_type_id
        )
        if not physical_object.geometry.is_valid:
            self._logger.warning("Invalid geometry in upload, fixing", geometry=physical_object.geometry)
            physical_object.geometry = physical_object.geometry.buffer(0)
        if not all(objects_around["geometry"].is_valid):
            self._logger.warning(
                "Invalid geometry got from Urban API, fixing", around_geometry=physical_object.geometry
            )
            objects_around["geometry"] = objects_around["geometry"].buffer(0)
        intersecting = self._get_intersecting_objects(physical_object.geometry, objects_around)

        if intersecting.shape[0] == 0:
            territory_id = await self._urban_client.get_common_territory_id(physical_object.geometry)
            if territory_id is None:
                return None

            result = await self._urban_client.upload_physical_object(
                PostPhysicalObject(
                    geometry=shapely_to_geometry(physical_object.geometry),
                    territory_id=territory_id,
                    physical_object_type_id=physical_object.physical_object_type_id,
                    centre_point=None,
                    address=physical_object.address,
                    name=physical_object.name,
                    properties=physical_object.properties,
                )
            )

            object_geometry_id = result.object_geometry.object_geometry_id
            for _, row in self._get_covered_objects(physical_object.geometry, objects_around).iterrows():
                await self._logger.adebug(
                    "Updating covered urban_object geometry", resulting_geometry_id=object_geometry_id
                )
                try:
                    covered_urban_object = await self._urban_client.get_urban_object_by_composite(
                        row["physical_object_id"], row["object_geometry_id"], None
                    )
                    await self._urban_client.patch_urban_object(
                        covered_urban_object.urban_object_id, object_geometry_id=object_geometry_id
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    await self._logger.awarning(
                        "Failed to update geometry of covered urban_object",
                        error=repr(exc),
                        error_type=type(exc),
                    )

            return result, True

        physical_object_id = intersecting.iloc[0]["physical_object_id"]

        geometries = await self._urban_client.get_physical_object_geometries(physical_object_id)
        geometries = self._get_intersecting_objects(physical_object.geometry, geometries)
        geometry_id = geometries.iloc[0]["object_geometry_id"]
        return await self._urban_client.get_urban_object_by_composite(physical_object_id, geometry_id, None), False

    async def _uploading_worker_func(
        self,
        upload_physical_object: Callable[[PhysicalObjectForUpload], Awaitable[tuple[UrbanObject, bool] | None]],
        worker_name: str | None = None,
    ) -> None:
        worker_logger = self._logger
        if worker_name is not None:
            worker_logger = worker_logger.bind(worker=worker_name)
        while True:
            try:
                physical_object = self._helper.get_row_for_upload()
                if physical_object is None:
                    worker_logger.info("no more objects to upload, finishing")
                    return
            except Exception as exc:  # pylint: disable=broad-except
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            try:
                result = await upload_physical_object(physical_object)
                if result is None:
                    self._helper.set_upload_error(physical_object.id, "impossible to upload", non_retryable=True)
                    continue
                urban_object, new_uploaded = result
            except KeyboardInterrupt:
                worker_logger.info("finishing on interruption")
                return
            except Exception as exc:  # pylint: disable=broad-except
                self._helper.set_upload_error(physical_object.id, repr(exc))
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            self._helper.set_upload_result(
                physical_object.id,
                urban_object.physical_object.physical_object_id,
                urban_object.object_geometry.object_geometry_id,
                not new_uploaded,
            )

    def _get_intersecting_objects(
        self,
        geometry: shapely.geometry.base.BaseGeometry,
        objects_around: gpd.GeoDataFrame,
        intersection_area_boundary: float = 0.6,
    ) -> gpd.GeoDataFrame:
        if objects_around.shape[0] == 0:
            return objects_around
        around_3857: gpd.GeoDataFrame = objects_around.to_crs(3857)
        if geometry.geom_type != "Point":
            around_3857 = around_3857[around_3857.geometry.area > 0]
        around_3857["geometry"] = around_3857["geometry"].buffer(5)
        geometry_3857 = transform_geometry_4326_to_3857(geometry).buffer(5)
        around_3857 = around_3857[around_3857.intersects(geometry_3857) | around_3857.contains(geometry_3857.centroid)]
        intersection_area: pd.Series = around_3857.intersection(geometry_3857).area
        around_3857["intersection"] = np.maximum(
            intersection_area / geometry_3857.area, intersection_area / around_3857.area
        )

        intersecting = objects_around.copy()
        intersecting["intersection"] = around_3857["intersection"]
        intersecting = intersecting[intersecting["intersection"] > intersection_area_boundary]

        return intersecting.sort_values("intersection", ascending=False)

    def _get_covered_objects(
        self, geometry: shapely.geometry.base.BaseGeometry, objects_around: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        around_3857: gpd.GeoDataFrame = objects_around.to_crs(3857)
        around_3857["geometry"] = around_3857["geometry"]
        geometry_3857 = transform_geometry_4326_to_3857(geometry).buffer(5)

        around_3857 = around_3857[around_3857.covered_by(geometry_3857) | around_3857.covers(geometry_3857.centroid)]
        return objects_around[objects_around.index.isin(around_3857.index)]


class PhysicalObjectsHelper:
    """Class which helps to upload physical_objects."""

    def __init__(self, sqlite: SQLiteHelper, logger: structlog.stdlib.BoundLogger):
        self._sqlite = sqlite
        self._logger = logger

    def prepare_db(self) -> None:
        "Prepare SQLite database."

        self._sqlite.execute(
            "CREATE TABLE IF NOT EXISTS physical_objects_data ("
            "   id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "   filename TEXT NOT NULL,"
            "   added_at TIMESTAMPTZ,"
            "   locked_till TIMESTAMPTZ,"
            "   physical_object_type_id INTEGER NOT NULL,"
            "   address TEXT,"
            "   name TEXT,"
            "   properties TEXT,"
            "   geometry TEXT NOT NULL,"
            "   already_existed BOOLEAN,"
            "   error TEXT,"
            "   retry_count INTEGER DEFAULT 0,"
            "   physical_object_id INTEGER,"
            "   geometry_id INTEGER"
            ")"
        )

    def get_row_for_upload(self) -> PhysicalObjectForUpload | None:
        """Select a physical_object ready to be uploaded and set its locked_till 5 minutes to future."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "physical_objects_data",
            columns=["id", "physical_object_type_id", "address", "name", "properties", "geometry"],
            where=(
                "(physical_object_id IS NULL OR geometry_id IS NULL)"
                f" AND (locked_till IS NULL OR locked_till < '{now}')"
            ),
            order_by="retry_count, added_at, locked_till",
            limit=1,
        )
        if len(results) == 0:
            return None
        result = results[0]
        self._sqlite.update(
            "physical_objects_data",
            where=f"id = {result['id']}",
            non_quoted_set="retry_count = retry_count + 1",
            locked_till=datetime.datetime.now() + datetime.timedelta(minutes=5),
        )
        try:
            if isinstance(result["properties"], str):
                result["properties"] = json.loads(result["properties"])
            result["geometry"] = shapely.wkt.loads(result["geometry"])
        except Exception as exc:
            self._logger.exception("Error on getting physical_object from SQLite", id=result["id"])
            self.set_upload_error(result["id"], f"Error on get: {repr(exc)}")
            raise
        return PhysicalObjectForUpload(physical_object_id=None, geometry_id=None, **result)

    def get_row_by_id(self, physical_object_id: int) -> PhysicalObjectForUpload | None:
        """Get a single building for upload by id without checking and updating locked_till."""
        results = self._sqlite.select(
            "physical_objects_data",
            columns=[
                "id",
                "physical_object_type_id",
                "address",
                "name",
                "properties",
                "geometry",
                "physical_object_id",
                "geometry_id",
            ],
            where=f"id = {physical_object_id}",
            limit=1,
        )
        if len(results) == 0:
            return None
        result = results[0]
        try:
            if isinstance(result["properties"], str):
                result["properties"] = json.loads(result["properties"])
            result["geometry"] = shapely.wkt.loads(result["geometry"])
        except Exception as exc:
            self._logger.exception("Error on getting physical_object from SQLite", id=result["id"])
            self.set_upload_error(result["id"], f"Error on get: {repr(exc)}")
            raise AlreadyLoggedException() from exc
        return PhysicalObjectForUpload(**result)

    def set_upload_result(
        self,
        physical_object_id: int,
        physical_object_external_id: int,
        geometry_external_id: int,
        already_existed: bool,
    ):
        """Set physical_object and geometry identifiers after uploading."""
        self._sqlite.update(
            "physical_objects_data",
            where=f"id = {physical_object_id}",
            physical_object_id=physical_object_external_id,
            geometry_id=geometry_external_id,
            already_existed=already_existed,
        )

    def set_upload_error(self, physical_object_id: int, error: str, non_retryable: bool = False):
        """Set error message for the given physical_object. `non_retryable` flag will also move locked_till
        5 days to future.
        """
        to_update = {"error": error}
        if non_retryable:
            to_update["locked_till"] = datetime.datetime.now() + datetime.timedelta(days=5)
        self._sqlite.update("physical_objects_data", where=f"id = {physical_object_id}", **to_update)

    def get_total(self) -> int:
        """Return total number of physical_objects ready to be uploaded at the moment."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "physical_objects_data",
            columns=["count(*)"],
            where=f"physical_object_id IS NULL AND (locked_till IS NULL OR locked_till < '{now}')",
        )
        return results[0]["count(*)"]
