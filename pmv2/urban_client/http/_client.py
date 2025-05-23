"""Urban API HTTP Client is defined here."""

import asyncio
from functools import wraps
from typing import Any, Callable

import geopandas as gpd
import pandas as pd
import shapely
import structlog.stdlib
from aiohttp import ClientConnectionError, ClientSession, ClientTimeout

from pmv2.urban_client._abstract import UrbanClient
from pmv2.urban_client.exceptions import APIConnectionError, APITimeoutError, ObjectNotFoundError
from pmv2.urban_client.http.exceptions import InvalidStatusCode
from pmv2.urban_client.http.models import Paginated
from pmv2.urban_client.models import (
    FunctionalZone,
    FunctionalZoneType,
    ObjectGeometry,
    PhysicalObject,
    PhysicalObjectType,
    PostFunctionalZone,
    PostPhysicalObject,
    PostService,
    Service,
    ServiceType,
    TerritoryWithoutGeometry,
    UrbanObject,
)


def _handle_exceptions(func: Callable) -> Callable:
    @wraps(func)
    async def _wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ClientConnectionError as exc:
            raise APIConnectionError("Error on connection to Urban API") from exc
        except asyncio.exceptions.TimeoutError as exc:
            raise APITimeoutError("Timeout expired on Urban API request") from exc

    return _wrapper


class HTTPUrbanClient(UrbanClient):
    """Urban API client that uses HTTP/HTTPS as transport."""

    def __init__(
        self,
        host: str,
        *,
        ping_timeout_seconds: float = 2.0,
        operation_timeout_sconds: float = 30.0,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        if logger is ...:
            logger = structlog.get_logger()
        if not host.startswith("http"):
            logger.warning("http/https schema is not set, defaulting to http")
            host = f"http://{host}"
        self._host = host
        self._logger = logger.bind(host=self._host)
        self._ping_timeout = ping_timeout_seconds
        self._operation_timeout = operation_timeout_sconds

    async def is_alive(self) -> bool:
        """Check if Urban API instance is responding."""
        async with self._get_session() as session:
            try:
                resp = await session.get("/health_check/ping", timeout=ClientTimeout(self._ping_timeout))
            except ClientConnectionError as exc:
                await self._logger.awarning("error on ping", error=repr(exc))
                return False
            except asyncio.exceptions.TimeoutError:
                await self._logger.awarning("timeout on ping")
                return False
            if resp.status == 200 and (await resp.json()) == {"message": "Pong!"}:
                return True
            await self._logger.awarning("error on ping", resp_code=resp.status, resp_text=await resp.text())
        return False

    @_handle_exceptions
    async def get_version(self) -> str:
        """Get Urban API version from OpenAPI specification."""
        async with self._get_session() as session:
            resp = await session.get("/api/openapi")
            if resp.status == 200:
                return (await resp.json())["info"]["version"]
            raise APIConnectionError("invalid response from /api/openapi")

    @_handle_exceptions
    async def get_objects_around(
        self, geom: shapely.geometry.base.BaseGeometry, physical_object_type_id: int | None = None
    ) -> gpd.GeoDataFrame:
        """Get physical objects around given geometry from Urban API."""
        body = shapely.geometry.mapping(geom)
        params = {}
        if physical_object_type_id is not None:
            params["physical_object_type_id"] = physical_object_type_id
        await self._logger.adebug("executing get_objects_around", body=body, params=params)
        async with self._get_session() as session:
            resp = await session.post("/api/v1/physical_objects/around", params=params, json=body)
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_objects_around", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_objects_around: got {resp.status}")
            df = pd.DataFrame(await resp.json())
            if df.shape[0] == 0:
                return gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=4326)
            df["geometry"] = df["geometry"].apply(shapely.geometry.shape)
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=4326)
        return gdf

    @_handle_exceptions
    async def get_urban_object(self, urban_object_id: int) -> UrbanObject | None:
        path = f"/api/v1/urban_objects/{urban_object_id}"
        await self._logger.adebug("executing get_urban_object", path=path)
        async with self._get_session() as session:
            resp = await session.get(path)
            if resp.status == 404:
                return None
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_urban_object", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_urban_object: got {resp.status}")
            urban_object = UrbanObject.model_validate_json(await resp.text())
            return urban_object

    @_handle_exceptions
    async def get_object_geometry(self, object_geometry_id: int) -> ObjectGeometry | None:
        path = "/api/v1/object_geometries"
        params = {"object_geometries_ids": object_geometry_id}
        await self._logger.adebug("executing get_object_geometry", path=path, params=params)
        async with self._get_session() as session:
            resp = await session.get(path, params=params)
            if resp.status == 404:
                return None
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_object_geometry", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_object_geometry: got {resp.status}")
            object_geometry = [ObjectGeometry.model_validate(og) for og in await resp.json()][0]
            return object_geometry

    @_handle_exceptions
    async def patch_urban_object(
        self,
        urban_object_id: int,
        object_geometry_id: int = ...,
        physical_object_id: int = ...,
        service_id: int | None = ...,
    ) -> UrbanObject:
        if object_geometry_id is ... and physical_object_id is ... and service_id is ...:
            return await self.get_urban_object(urban_object_id)
        body = dict(
            filter(
                lambda kv: kv[1] is not ...,
                {
                    "object_geometry_id": object_geometry_id,
                    "physical_object_id": physical_object_id,
                    "service_id": service_id,
                }.items(),
            )
        )
        await self._logger.adebug("executing patch_urban_object", body=body, urban_object_id=urban_object_id)
        async with self._get_session() as session:
            resp = await session.patch(f"/api/v1/urban_objects/{urban_object_id}", json=body)
            if resp.status == 404:
                raise ObjectNotFoundError()
            if resp.status != 200:
                await self._logger.aerror(
                    "error on patch_urban_object", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on patch_urban_object: got {resp.status}")
            urban_object = UrbanObject.model_validate_json(await resp.text())
            return urban_object

    @_handle_exceptions
    async def patch_object_geometry(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        object_geometry_id: int,
        geometry: shapely.geometry.base.BaseGeometry = ...,
        territory_id: int = ...,
        address: str = ...,
        osm_id: str = ...,
    ) -> ObjectGeometry:
        if geometry is ... and territory_id is ... and address is ... and osm_id is ...:
            return await self.get_object_geometry(object_geometry_id)
        body = dict(
            filter(
                lambda kv: kv[1] is not ...,
                {
                    "object_geometry_id": object_geometry_id,
                    "geometry": shapely.geometry.mapping(geometry) if geometry is not ... else ...,
                    "territory_id": territory_id,
                    "address": address,
                    "osm_id": osm_id,
                }.items(),
            )
        )
        await self._logger.adebug("executing patch_object_geometry", body=body, object_geometry_id=object_geometry_id)
        async with self._get_session() as session:
            resp = await session.patch(f"/api/v1/object_geometries/{object_geometry_id}", json=body)
            if resp.status == 404:
                raise ObjectNotFoundError()
            if resp.status != 200:
                await self._logger.aerror(
                    "error on patch_object_geometry", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on patch_object_geometry: got {resp.status}")
            object_geometry = ObjectGeometry.model_validate_json(await resp.text())
            return object_geometry

    @_handle_exceptions
    async def get_urban_object_by_composite(
        self, physical_object_id: int, object_geometry_id: int, service_id: int | None
    ) -> UrbanObject | None:
        path = f"/api/v1/urban_objects_by_physical_object?physical_object_id={physical_object_id}"
        await self._logger.adebug("executing get_urban_object_by_composite", path=path)
        async with self._get_session() as session:
            resp = await session.get(path)
            if resp.status == 404:
                return None
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_urban_object_by_composite", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_urban_object_by_composite: got {resp.status}")
            urban_objects = [UrbanObject.model_validate(entry) for entry in await resp.json()]
        potential: UrbanObject | None = None
        for ub in urban_objects:
            if (
                ub.physical_object.physical_object_id == physical_object_id
                and ub.object_geometry.object_geometry_id == object_geometry_id
            ):
                if service_id is None:
                    if ub.service is None:
                        return ub
                    potential = ub
                elif ub.service is not None and ub.service.service_id == service_id:
                    return ub
        if potential is not None:
            potential.service = None
            return potential
        return None

    @_handle_exceptions
    async def get_physical_object_geometries(self, physical_object_id: int) -> gpd.GeoDataFrame:
        path = f"/api/v1/physical_objects/{physical_object_id}/geometries"
        await self._logger.adebug("executing get_physical_object_geometries", path=path)
        async with self._get_session() as session:
            resp = await session.get(path)
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_physical_object_geometries", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_physical_object_geometries: got {resp.status}")
            df = pd.DataFrame(await resp.json())
            df["geometry"] = df["geometry"].apply(shapely.geometry.shape)
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs=4326)
        return gdf

    @_handle_exceptions
    async def get_physical_object_services(
        self, physical_object_id: int, service_type_id: int | None = None
    ) -> list[Service]:
        path = f"/api/v1/physical_objects/{physical_object_id}/services"
        params = {}
        if service_type_id is not None:
            params["service_type_id"] = service_type_id
        await self._logger.adebug("executing get_physical_object_services", path=path, params=params)
        async with self._get_session() as session:
            resp = await session.get(path, params=params)
            if resp.status == 404:
                return []
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_physical_object_services", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_physical_object_services: got {resp.status}")
            services = [Service.model_validate(s) for s in await resp.json()]
            return services

    @_handle_exceptions
    async def get_physical_object_types(self) -> list[PhysicalObjectType]:
        async with self._get_session() as session:
            resp = await session.get("/api/v1/physical_object_types")
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_physical_object_types", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_physical_object_types: got {resp.status}")
            result = [PhysicalObjectType.model_validate(entry) for entry in await resp.json()]
        return result

    @_handle_exceptions
    async def get_service_types(self) -> list[ServiceType]:
        async with self._get_session() as session:
            resp = await session.get("/api/v1/service_types")
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_service_types", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_service_types: got {resp.status}")
            result = [ServiceType.model_validate(entry) for entry in await resp.json()]
        return result

    @_handle_exceptions
    async def upload_physical_object(self, physycal_object: PostPhysicalObject) -> UrbanObject:
        body = physycal_object.model_dump(mode="json")
        await self._logger.adebug("executing upload_physical_object", body=body)
        async with self._get_session() as session:
            resp = await session.post("/api/v1/physical_objects", json=body)
            if resp.status != 201:
                await self._logger.aerror(
                    "error on upload_physical_object", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on upload_physical_object: got {resp.status}")
            result = UrbanObject.model_validate_json(await resp.text())
        return result

    @_handle_exceptions
    async def add_building(  # pylint: disable=too-many-arguments
        self,
        physical_object_id: int,
        *,
        floors: int | None,
        building_area_official: float | None,
        building_area_modeled: float | None,
        project_type: str | None,
        floor_type: str | None,
        wall_material: str | None,
        built_year: int | None,
        exploitation_start_year: int | None,
        properties: dict[str, Any],
    ) -> PhysicalObject:
        body = {
            "physical_object_id": physical_object_id,
            "floors": floors,
            "building_area_official": building_area_official,
            "building_area_modeled": building_area_modeled,
            "project_type": project_type,
            "floor_type": floor_type,
            "wall_material": wall_material,
            "built_year": built_year,
            "exploitation_start_year": exploitation_start_year,
            "properties": properties,
        }
        await self._logger.adebug("executing add_building", body=body)
        async with self._get_session() as session:
            resp = await session.post("/api/v1/buildings", json=body)
            if resp.status != 201:
                await self._logger.aerror("error on add_building", resp_code=resp.status, resp_text=await resp.text())
                raise InvalidStatusCode(f"Unexpected status code on add_building: {resp.status}")
            result = PhysicalObject.model_validate_json(await resp.text())
        return result

    @_handle_exceptions
    async def upload_service(self, service: PostService) -> Service:
        body = service.model_dump(mode="json")
        await self._logger.adebug("executing upload_service", body=body)
        async with self._get_session() as session:
            resp = await session.post("/api/v1/services", json=body)
            if resp.status != 201:
                await self._logger.aerror("error on upload_service", resp_code=resp.status, resp_text=await resp.text())
                raise InvalidStatusCode(f"Unexpected status code on upload_service: {resp.status}")
            result = Service.model_validate_json(await resp.text())
        return result

    @_handle_exceptions
    async def get_inner_territories(self, territory_id: int | None) -> list[TerritoryWithoutGeometry]:
        clause = f"parent_id={territory_id}&" if territory_id is not None else ""
        path = f"/api/v2/territories_without_geometry?{clause}size=100"
        await self._logger.adebug("executing get_inner_territories", path=path)
        async with self._get_session() as session:
            resp = await session.get(path)
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_inner_territories", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_inner_territories: {resp.status}")
            result = Paginated[TerritoryWithoutGeometry].model_validate_json(await resp.text())
            return await result.get_all_pages(session)

    @_handle_exceptions
    async def get_common_territory_id(self, geom: shapely.geometry.base.BaseGeometry) -> int | None:
        body = shapely.geometry.mapping(geom)

        await self._logger.adebug("executing get_common_territory", body=body)

        async with self._get_session() as session:
            resp = await session.post("/api/v1/common_territory", json=body)
            match resp.status:
                case 200:
                    result = await resp.json()
                    return result.get("territory_id")
                case 404:
                    return None
                case _:
                    await self._logger.aerror(
                        "error on get_common_territory", resp_code=resp.status, resp_text=await resp.text()
                    )
                    raise InvalidStatusCode(f"Unexpected status code on get_common_territory: got {resp.status}")

    @_handle_exceptions
    async def get_functional_zone_types(self) -> list[FunctionalZoneType]:
        path = "/api/v1/functional_zones_types"
        await self._logger.adebug("executing get_functional_zone_types", path=path)
        async with self._get_session() as session:
            resp = await session.get(path)
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_functional_zone_types", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_functional_zone_types: {resp.status}")
            return [FunctionalZoneType.model_validate(entry) for entry in await resp.json()]

    @_handle_exceptions
    async def get_functional_zones(
        self,
        territory_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None = None,
    ) -> list[FunctionalZone]:
        path = f"/api/v1/territory/{territory_id}/functional_zones"
        params = {
            "year": year,
            "source": source,
            "functional_zone_type_id": functional_zone_type_id,
        }
        await self._logger.adebug("executing get_functional_zones", path=path, params=params)
        async with self._get_session() as session:
            resp = await session.get(path, params=params)
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_functional_zones", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_functional_zones: {resp.status}")
            return [FunctionalZone.model_validate(entry) for entry in await resp.json()]

    @_handle_exceptions
    async def get_functional_zones_around(
        self,
        geom: shapely.geometry.base.BaseGeometry,
        year: int,
        source: str,
        functional_zone_type_id: int | None = None,
    ) -> list[FunctionalZone]:
        path = f"/api/v1/functional_zones/around"
        body = shapely.geometry.mapping(geom)
        params = {
            "year": year,
            "source": source,
            "functional_zone_type_id": functional_zone_type_id,
        }
        await self._logger.adebug("executing get_functional_zones_around", path=path, params=params)
        async with self._get_session() as session:
            resp = await session.post(path, params=params, json=body)
            if resp.status != 200:
                await self._logger.aerror(
                    "error on get_functional_zones_around", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on get_functional_zones_around: {resp.status}")
            return [FunctionalZone.model_validate(entry) for entry in await resp.json()]

    @_handle_exceptions
    async def upload_functional_zone(self, functional_zone: PostFunctionalZone) -> FunctionalZone:
        body = functional_zone.model_dump(mode="json")
        await self._logger.adebug("executing upload_functional_zone", body=body)
        async with self._get_session() as session:
            resp = await session.post(
                "/api/v1/functional_zones",
                json=body,
            )
            if resp.status != 201:
                await self._logger.aerror(
                    "error on upload_functional_zone", resp_code=resp.status, resp_text=await resp.text()
                )
                raise InvalidStatusCode(f"Unexpected status code on upload_functional_zone: {resp.status}")
            result = FunctionalZone.model_validate_json(await resp.text())
        return result

    def _get_session(self) -> ClientSession:
        return ClientSession(self._host, timeout=ClientTimeout(self._operation_timeout))
