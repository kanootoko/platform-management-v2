"""Buildings upload logic is defined here."""

import asyncio
import datetime
import json
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import geopandas as gpd
import pandas as pd
import structlog

from pmv2.logic.sqlite import SQLiteHelper
from pmv2.logic.upload_physical_objects import PhysicalObjectsUploader
from pmv2.logic.utils import AlreadyLoggedException, logging_wrapper
from pmv2.urban_client import UrbanClient
from pmv2.urban_client.http.exceptions import InvalidStatusCode
from pmv2.urban_client.models import UrbanObject


@dataclass
class BuildingForUpload:  # pylint: disable=too-many-instance-attributes
    """Building prepared for upload from SQLite database."""

    id: int
    floors: int | None
    building_area_official: float | None
    building_area_modeled: float | None
    project_type: str | None
    floor_type: str | None
    wall_material: str | None
    built_year: int | None
    exploitation_start_year: int | None
    properties: dict[str, Any]
    physical_object_id: int
    physical_object_id_external: int


class BuildingsUploader:
    """Buildings uploader."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        urban_client: UrbanClient,
        *,
        sqlite: SQLiteHelper,
        po_uploader: PhysicalObjectsUploader,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self._po_uploader = po_uploader
        self._sqlite = sqlite
        if logger is ...:
            self._logger = structlog.get_logger("upload_buildings")
        else:
            self._logger = logger
        self._helper = BuildingsHelper(sqlite, self._logger)
        self._helper.prepare_db()

    async def prepare_buildings(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        gdf: gpd.GeoDataFrame,
        *,
        filename: str,
        physical_object_type_mapper: Callable[[dict[str, Any]], tuple[int, bool | None]],
        floors_mapper: Callable[[dict[str, Any]], tuple[int | None, Callable[[dict[str, Any]], None]]],
        building_area_official_mapper: Callable[
            [dict[str, Any]], tuple[float | None, Callable[[dict[str, Any]], None]]
        ],
        building_area_modeled_mapper: Callable[[dict[str, Any]], tuple[float | None, Callable[[dict[str, Any]], None]]],
        project_type_mapper: Callable[[dict[str, Any]], tuple[str | None, Callable[[dict[str, Any]], None]]],
        floor_type_mapper: Callable[[dict[str, Any]], tuple[str | None, Callable[[dict[str, Any]], None]]],
        wall_material_mapper: Callable[[dict[str, Any]], tuple[str | None, Callable[[dict[str, Any]], None]]],
        built_year_mapper: Callable[[dict[str, Any]], tuple[int | None, Callable[[dict[str, Any]], None]]],
        exploitation_start_year_mapper: Callable[[dict[str, Any]], tuple[int | None, Callable[[dict[str, Any]], None]]],
        building_properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
        po_data_mapper: (
            Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]] | None
        ) = None,
        po_osm_id_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_address_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_name_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
    ) -> list[int]:
        """Insert buildings+physical_objects with a physical_object_type_id set by mapper in the SQLite database."""
        now = datetime.datetime.now()

        physical_objects_data: list[dict[str, Any]] = []
        buildings_data: list[dict[str, Any]] = []

        for _, full_series in gdf.iterrows():
            full_data = full_series.dropna().to_dict()
            physical_object_type_id, _ = physical_object_type_mapper(full_data)

            physical_object_data, cb = po_data_mapper(full_data)
            physical_object_data["physical_object_type_id"] = physical_object_type_id
            physical_objects_data.append(physical_object_data)
            cb(full_data)

            buildings_data.append(full_data)

        po_gdf = pd.DataFrame(physical_objects_data)
        po_gdf = gpd.GeoDataFrame(po_gdf, geometry="geometry", crs=gdf.crs)

        def physical_object_type_id_mapper(data: dict[str, Any]) -> tuple[int, Callable[[dict[str, Any]], None]]:
            def remove(data_again: dict[str, Any]) -> None:
                del data_again["physical_object_type_id"]

            return data["physical_object_type_id"], remove

        self._logger.debug("preparing physical_objects")

        physical_object_ids = await self._po_uploader.prepare_physical_objects(
            po_gdf,
            filename=filename,
            physical_object_type_id_mapper=physical_object_type_id_mapper,
            osm_id_mapper=po_osm_id_mapper,
            address_mapper=po_address_mapper,
            name_mapper=po_name_mapper,
            properties_mapper=po_properties_mapper,
        )

        to_insert: list[dict[str, Any]] = []

        for building_data, physical_object_id in zip(buildings_data, physical_object_ids):
            callbacks = []
            floors, cb = floors_mapper(building_data)
            callbacks.append(cb)
            building_area_official, cb = building_area_official_mapper(building_data)
            callbacks.append(cb)
            building_area_modeled, cb = building_area_modeled_mapper(building_data)
            callbacks.append(cb)
            project_type, cb = project_type_mapper(building_data)
            callbacks.append(cb)
            floor_type, cb = floor_type_mapper(building_data)
            callbacks.append(cb)
            wall_material, cb = wall_material_mapper(building_data)
            callbacks.append(cb)
            built_year, cb = built_year_mapper(building_data)
            callbacks.append(cb)
            exploitation_start_year, cb = exploitation_start_year_mapper(building_data)
            callbacks.append(cb)

            for cb in callbacks:
                cb(building_data)
            properties, cb = building_properties_mapper(building_data)
            cb(building_data)

            to_insert.append(
                {
                    "floors": floors,
                    "building_area_official": building_area_official,
                    "building_area_modeled": building_area_modeled,
                    "project_type": project_type,
                    "floor_type": floor_type,
                    "wall_material": wall_material,
                    "built_year": built_year,
                    "exploitation_start_year": exploitation_start_year,
                    "properties": properties,
                    "added_at": now,
                    "physical_object_id": physical_object_id,
                    "filename": filename,
                }
            )

        self._logger.debug("preparing buildings")

        inserted_ids = self._sqlite.insert_many(
            "buildings_data",
            data=to_insert,
            returning="id",
            columns=[
                "floors",
                "building_area_official",
                "building_area_modeled",
                "project_type",
                "floor_type",
                "wall_material",
                "built_year",
                "exploitation_start_year",
                "properties",
                "added_at",
                "physical_object_id",
                "filename",
            ],
        )

        return inserted_ids

    async def upload_buildings(self, parallel_workers: int = 1) -> tuple[list[UrbanObject], gpd.GeoDataFrame | None]:
        """Upload buildings from SQLite database in `parallel_workers` async workers."""
        total = self._helper.get_total()

        upload_func = logging_wrapper(
            self._logger, total, "preparing to upload building", self.upload_building_if_not_exists
        )
        workers = [self._uploading_worker_func(upload_func, worker_name=f"{i}") for i in range(parallel_workers)]

        await asyncio.gather(*workers)

    async def upload_building_if_not_exists(self, building: BuildingForUpload) -> tuple[UrbanObject, bool]:
        """Upload a building after checking that it does not exist - or return existing one."""
        result = await self._po_uploader.upload_one_if_not_exists(building.physical_object_id)
        if result is None:
            self._logger.warning("building has no territory parent", id=building.id)
            return None
        urban_object, _ = result

        if urban_object.physical_object.building is None:
            try:
                await self._urban_client.add_building(
                    urban_object.physical_object.physical_object_id,
                    floors=building.floors,
                    building_area_official=building.building_area_official,
                    building_area_modeled=building.building_area_modeled,
                    project_type=building.project_type,
                    floor_type=building.floor_type,
                    wall_material=building.wall_material,
                    built_year=building.built_year,
                    exploitation_start_year=building.exploitation_start_year,
                    properties=building.properties,
                )
            except InvalidStatusCode as exc:
                if ": 409" not in str(exc):
                    raise
        return result

    async def _uploading_worker_func(
        self,
        upload_building: Callable[[BuildingForUpload], Awaitable[tuple[UrbanObject, bool] | None]],
        worker_name: str | None = None,
    ) -> None:
        worker_logger = self._logger
        if worker_name is not None:
            worker_logger = worker_logger.bind(worker=worker_name)
        while True:
            try:
                building = self._helper.get_row_for_upload()
                if building is None:
                    worker_logger.info("no more buildings to upload, finishing")
                    return
            except Exception as exc:  # pylint: disable=broad-except
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            try:
                result = await upload_building(building)
                if result is None:
                    self._helper.set_upload_error(building.id, "impossible to upload", non_retryable=True)
                    continue
                urban_object, new_uploaded = result
            except KeyboardInterrupt:
                worker_logger.info("finishing on interruption")
                return
            except Exception as exc:  # pylint: disable=broad-except
                self._helper.set_upload_error(building.id, repr(exc))
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            self._helper.set_upload_result(
                building.id, urban_object.physical_object.physical_object_id, not new_uploaded
            )


class BuildingsHelper:
    """Class which helps to upload buildings."""

    def __init__(self, sqlite: SQLiteHelper, logger: structlog.stdlib.BoundLogger):
        self._sqlite = sqlite
        self._logger = logger

    def prepare_db(self) -> None:
        "Prepare SQLite database. Note: PhysicalObjectsHelper preparation should come before BuildingsHelper's."

        self._sqlite.execute(
            "CREATE TABLE IF NOT EXISTS buildings_data ("
            "   id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "   filename TEXT NOT NULL,"
            "   added_at TIMESTAMPTZ,"
            "   locked_till TIMESTAMPTZ,"
            "   floors INTEGER,"
            "   building_area_official FLOAT,"
            "   building_area_modeled FLOAT,"
            "   project_type TEXT,"
            "   floor_type TEXT,"
            "   wall_material TEXT,"
            "   built_year INTEGER,"
            "   exploitation_start_year INTEGER,"
            "   properties TEXT,"
            "   physical_object_id INTEGER REFERENCES physical_objects_data(id),"
            "   already_existed BOOLEAN,"
            "   error TEXT,"
            "   retry_count INTEGER DEFAULT 0,"
            "   building_id INTEGER"
            ")"
        )

    def get_row_for_upload(self) -> BuildingForUpload | None:
        """Select a building ready to be uploaded and set its locked_till 5 minutes to future."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "buildings_data b JOIN physical_objects_data po ON b.physical_object_id = po.id",
            columns=[
                "b.id",
                "b.floors",
                "b.building_area_official",
                "b.building_area_modeled",
                "b.project_type",
                "b.floor_type",
                "b.wall_material",
                "b.built_year",
                "b.exploitation_start_year",
                "b.properties",
                "po.id",
                "po.physical_object_id",
            ],
            where=f"b.building_id IS NULL AND (b.locked_till IS NULL OR b.locked_till < '{now}')",
            order_by="b.retry_count, b.added_at, b.locked_till",
            limit=1,
        )
        if len(results) == 0:
            return None
        result = results[0]
        self._sqlite.update(
            "buildings_data",
            where=f"id = {result['b.id']}",
            non_quoted_set="retry_count = retry_count + 1",
            locked_till=datetime.datetime.now() + datetime.timedelta(minutes=5),
        )
        try:
            if isinstance(result["b.properties"], str):
                result["b.properties"] = json.loads(result["b.properties"])
        except Exception as exc:
            self._logger.exception("Error on getting building from SQLite", id=result["id"])
            self.set_upload_error(result["id"], f"Error on get: {repr(exc)}")
            raise AlreadyLoggedException() from exc
        return BuildingForUpload(
            id=result["b.id"],
            floors=_try_int(result["b.floors"]),
            building_area_official=_try_float(result["b.building_area_official"]),
            building_area_modeled=_try_float(result["b.building_area_modeled"]),
            project_type=result["b.project_type"],
            floor_type=result["b.floor_type"],
            wall_material=result["b.wall_material"],
            built_year=_try_int(result["b.built_year"]),
            exploitation_start_year=_try_int(result["b.exploitation_start_year"]),
            properties=result["b.properties"] or {},
            physical_object_id=result["po.id"],
            physical_object_id_external=result["po.physical_object_id"],
        )

    def set_upload_result(self, building_id: int, external_id: int, already_existed: bool):
        """Set building id after uploading."""
        self._sqlite.update(
            "buildings_data",
            where=f"id = {building_id}",
            building_id=external_id,
            already_existed=already_existed,
        )

    def set_upload_error(self, building_id: int, error: str, non_retryable: bool = False):
        """Set error message for the given building. `non_retryable` flag will also move
        locked_till 5 days to future.
        """
        to_update = {"error": error}
        if non_retryable:
            to_update["locked_till"] = datetime.datetime.now() + datetime.timedelta(days=5)
        self._sqlite.update("buildings_data", where=f"id = {building_id}", **to_update)

    def get_total(self) -> int:
        """Return total number of buildings ready to be uploaded at the moment."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "buildings_data",
            columns=["count(*)"],
            where=f"building_id IS NULL AND (locked_till IS NULL OR locked_till < '{now}')",
        )
        return results[0]["count(*)"]


def _try_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            return float(val.replace(",", "."))
        return float(val)
    except ValueError:
        return None


def _try_int(val: Any) -> int | None:
    float_val = _try_float(val)
    if float_val is None:
        return None
    return int(float_val)
