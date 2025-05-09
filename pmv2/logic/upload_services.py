"""Services upload logic is defined here."""

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
from pmv2.logic.utils import AlreadyLoggedException, logging_wrapper, try_int, try_str
from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import PostService, Service, UrbanObject


@dataclass
class ServiceForUpload:  # pylint: disable=too-many-instance-attributes
    """Service prepared for upload from SQLite database."""

    id: int
    name: str
    service_type_id: int
    capacity: int | None
    properties: dict[str, Any]
    physical_object_id: int
    physical_object_id_external: int
    geometry_id_external: int


class ServicesUploader:
    """Services uploader."""

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
            self._logger = structlog.get_logger("upload_pysical_objects")
        else:
            self._logger = logger
        self._helper = ServicesHelper(sqlite, self._logger)
        self._helper.prepare_db()

    async def prepare_services(  # pylint: disable=too-many-arguments,too-many-locals
        self,
        gdf: gpd.GeoDataFrame,
        *,
        filename: str,
        service_type_id: int,
        physical_object_type_id: int,
        service_name_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        service_properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
        service_capacity_mapper: Callable[[dict[str, Any]], tuple[int, Callable[[dict[str, Any]], None]]],
        po_data_mapper: (
            Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]] | None
        ) = None,
        po_osm_id_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_address_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_name_mapper: Callable[[dict[str, Any]], tuple[str, Callable[[dict[str, Any]], None]]],
        po_properties_mapper: Callable[[dict[str, Any]], tuple[dict[str, Any], Callable[[dict[str, Any]], None]]],
    ) -> list[int]:
        """Upload GeoDataFrame of services of the same service_type."""
        now = datetime.datetime.now()

        physical_objects_data: list[dict[str, Any]] = []
        services_data: list[dict[str, Any]] = []

        for _, full_series in gdf.iterrows():
            full_data = full_series.dropna().to_dict()

            physical_object_data, cb = po_data_mapper(full_data)
            physical_object_data["physical_object_type_id"] = physical_object_type_id
            physical_objects_data.append(physical_object_data)
            cb(full_data)

            services_data.append(full_data)

        po_gdf = pd.DataFrame(physical_objects_data)
        po_gdf = gpd.GeoDataFrame(po_gdf, geometry="geometry", crs=gdf.crs)

        def physical_object_type_id_mapper(_: dict[str, Any]) -> tuple[int, Callable[[dict[str, Any]], None]]:

            return physical_object_type_id, lambda _: None

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

        for service_data, physical_object_id in zip(services_data, physical_object_ids):
            callbacks = []
            name, cb = service_name_mapper(service_data)
            callbacks.append(cb)
            capacity, cb = service_capacity_mapper(service_data)
            callbacks.append(cb)
            properties, cb = service_properties_mapper(service_data)
            callbacks.append(cb)

            for cb in callbacks:
                cb(service_data)

            to_insert.append(
                {
                    "name": name,
                    "capacity": capacity,
                    "properties": properties,
                    "service_type_id": service_type_id,
                    "added_at": now,
                    "physical_object_id": physical_object_id,
                    "filename": filename,
                }
            )

        self._logger.debug("preparing services")

        inserted_ids = self._sqlite.insert_many(
            "services_data",
            data=to_insert,
            returning="id",
            columns=[
                "name",
                "capacity",
                "properties",
                "service_type_id",
                "added_at",
                "physical_object_id",
                "filename",
            ],
        )

        return inserted_ids

    async def upload_services(self, parallel_workers: int = 1) -> tuple[list[UrbanObject], gpd.GeoDataFrame | None]:
        """Upload services from SQLite database in `parallel_workers` async workers."""
        total = self._helper.get_total()

        upload_func = logging_wrapper(
            self._logger, total, "preparing to upload service", self.upload_service_if_not_exists
        )
        workers = [self._uploading_worker_func(upload_func, worker_name=f"{i}") for i in range(parallel_workers)]

        await asyncio.gather(*workers)

    async def upload_service_if_not_exists(self, service: ServiceForUpload) -> tuple[Service, bool] | None:
        """Upload a service after checking that it does not exist - or return existing one."""
        physical_object = await self._po_uploader.upload_one_if_not_exists(service.physical_object_id)
        if physical_object is None:
            self._logger.warning("service has no territory parent", id=service.id)
            return None
        urban_object, _ = physical_object

        services = await self._urban_client.get_physical_object_services(
            urban_object.physical_object.physical_object_id, service_type_id=service.service_type_id
        )
        for s in services:
            if s.name == service.name:
                return s, False

        s = await self._urban_client.upload_service(
            PostService(
                physical_object_id=urban_object.physical_object.physical_object_id,
                object_geometry_id=urban_object.object_geometry.object_geometry_id,
                service_type_id=service.service_type_id,
                territory_type_id=None,
                name=service.name,
                capacity=service.capacity,
                properties=service.properties,
            )
        )
        return s, True

    async def _uploading_worker_func(
        self,
        upload_service: Callable[[ServiceForUpload], Awaitable[tuple[Service, bool] | None]],
        worker_name: str | None = None,
    ) -> tuple[list[Service], gpd.GeoDataFrame | None]:
        worker_logger = self._logger
        if worker_name is not None:
            worker_logger = worker_logger.bind(worker=worker_name)
        while True:
            try:
                service = self._helper.get_row_for_upload()
                if service is None:
                    worker_logger.info("no more services to upload, finishing")
                    return
            except Exception as exc:  # pylint: disable=broad-except
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            try:
                result = await upload_service(service)
                if result is None:
                    self._helper.set_upload_error(service.id, "impossible to upload", non_retryable=True)
                    continue
                service_uploaded, new_uploaded = result
            except KeyboardInterrupt:
                worker_logger.info("finishing on interruption")
                return
            except Exception as exc:  # pylint: disable=broad-except
                self._helper.set_upload_error(service.id, repr(exc))
                if not isinstance(exc, AlreadyLoggedException):
                    worker_logger.exception("error on upload")
                continue
            self._helper.set_upload_result(service.id, service_uploaded.service_id, not new_uploaded)


class ServicesHelper:
    """Class which helps to upload buildings."""

    def __init__(self, sqlite: SQLiteHelper, logger: structlog.stdlib.BoundLogger):
        self._sqlite = sqlite
        self._logger = logger

    def prepare_db(self) -> None:
        "Prepare SQLite database. Note: PhysicalObjectsHelper preparation should come before ServicesHelper's"

        self._sqlite.execute(
            "CREATE TABLE IF NOT EXISTS services_data ("
            "   id INTEGER PRIMARY KEY AUTOINCREMENT,"
            "   filename TEXT NOT NULL,"
            "   added_at TIMESTAMPTZ,"
            "   locked_till TIMESTAMPTZ,"
            "   name TEXT,"
            "   capacity INTEGER,"
            "   properties TEXT,"
            "   service_type_id INTEGER,"
            "   physical_object_id INTEGER REFERENCES physical_objects_data(id),"
            "   already_existed BOOLEAN,"
            "   error TEXT,"
            "   retry_count INTEGER DEFAULT 0,"
            "   service_id INTEGER"
            ")"
        )

    def get_row_for_upload(self) -> ServiceForUpload | None:
        """Select a service ready to be uploaded and set its locked_till 5 minutes to future."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "services_data s JOIN physical_objects_data po ON s.physical_object_id = po.id",
            columns=[
                "s.id",
                "s.name",
                "s.capacity",
                "s.service_type_id",
                "s.properties",
                "po.id",
                "po.physical_object_id",
                "po.geometry_id",
            ],
            where=f"s.service_id IS NULL AND (s.locked_till IS NULL OR s.locked_till < '{now}')",
            order_by="s.retry_count, s.added_at, s.locked_till",
            limit=1,
        )
        if len(results) == 0:
            return None
        result = results[0]
        self._sqlite.update(
            "services_data",
            where=f"id = {result['s.id']}",
            non_quoted_set="retry_count = retry_count + 1",
            locked_till=datetime.datetime.now() + datetime.timedelta(minutes=5),
        )
        try:
            if isinstance(result["s.properties"], str):
                result["s.properties"] = json.loads(result["s.properties"])
        except Exception as exc:
            self._logger.exception("Error on getting service from SQLite", id=result["id"])
            self.set_upload_error(result["id"], f"Error on get: {repr(exc)}")
            raise AlreadyLoggedException() from exc
        return ServiceForUpload(
            id=int(result["s.id"]),
            name=try_str(result["s.name"]),
            capacity=try_int(result["s.capacity"]),
            service_type_id=int(result["s.service_type_id"]),
            properties=result["s.properties"],
            physical_object_id=try_int(result["po.id"]),
            physical_object_id_external=try_int(result["po.physical_object_id"]),
            geometry_id_external=try_int(result["po.geometry_id"]),
        )

    def set_upload_result(self, service_id: int, external_id: int, already_existed: bool):
        """Set service id after uploading."""
        self._sqlite.update(
            "services_data",
            where=f"id = {service_id}",
            service_id=external_id,
            already_existed=already_existed,
        )

    def set_upload_error(self, service_id: int, error: str, non_retryable: bool = False):
        """Set error message for the given service. `non_retryable` flag will also move locked_till 5 days to future."""
        to_update = {"error": error}
        if non_retryable:
            to_update["locked_till"] = datetime.datetime.now() + datetime.timedelta(days=5)
        self._sqlite.update("services_data", where=f"id = {service_id}", **to_update)

    def get_total(self) -> int:
        """Return total number of services ready to be uploaded at the moment."""
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        results = self._sqlite.select(
            "services_data",
            columns=["count(*)"],
            where=f"service_id IS NULL AND (locked_till IS NULL OR locked_till < '{now}')",
        )
        return results[0]["count(*)"]
