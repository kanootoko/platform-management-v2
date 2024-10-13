"""Urban API HTTP Client is defined here."""

import asyncio
from functools import wraps
from typing import Callable

import geopandas as gpd
import pandas as pd
import shapely
import structlog.stdlib
from aiohttp import ClientConnectionError, ClientSession, ClientTimeout

from pmv2.urban_client._abstract import UrbanClient
from pmv2.urban_client.exceptions import APIConnectionError, APITimeoutError
from pmv2.urban_client.http.exceptions import InvalidStatusCode
from pmv2.urban_client.http.models import Paginated
from pmv2.urban_client.models import (
    PhysicalObjectType,
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

    def __init__(self, host: str, logger: structlog.stdlib.BoundLogger = ...):
        if logger is ...:
            logger = structlog.get_logger()
        if not host.startswith("http"):
            logger.warning("http/https schema is not set, defaulting to http")
            host = f"http://{host}"
        self._host = host
        self._logger = logger.bind(host=self._host)

    async def is_alive(self) -> bool:
        """Check if Urban API instance is responding."""
        async with self._get_session() as session:
            try:
                resp = await session.get("/health_check/ping", timeout=10)
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
        await self._logger.adebug("executing get_objects_around", body=body)
        clause = ""
        if physical_object_type_id is not None:
            clause = f"?physical_object_type_id={physical_object_type_id}"
        async with self._get_session() as session:
            resp = await session.post(f"/api/v1/physical_objects/around{clause}", json=body)
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

    def _get_session(self) -> ClientSession:
        return ClientSession(self._host, timeout=ClientTimeout(20))
