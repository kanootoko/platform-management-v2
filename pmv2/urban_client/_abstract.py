"""Abstract protocol for Urban API client is defined here."""

import abc
from typing import Any

import geopandas as gpd
import shapely

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


class UrbanClient(abc.ABC):
    """Urban API client"""

    @abc.abstractmethod
    async def is_alive(self) -> bool:
        """Check if urban_api instance is alive."""

    async def get_version(self) -> str | None:
        """Get API version if appliable."""
        return None

    @abc.abstractmethod
    async def get_objects_around(
        self, geom: shapely.geometry.base.BaseGeometry, physical_object_type_id: int | None = None
    ) -> gpd.GeoDataFrame:
        """Get physical objects around given geometry."""

    @abc.abstractmethod
    async def get_urban_object(self, urban_object_id: int) -> UrbanObject | None:
        """Get urban object by its identifier."""

    @abc.abstractmethod
    async def get_object_geometry(self, object_geometry_id: int) -> ObjectGeometry | None:
        """Get object_geometry by its identifier."""

    @abc.abstractmethod
    async def patch_urban_object(
        self,
        urban_object_id: int,
        object_geometry_id: int = ...,
        physical_object_id: int = ...,
        service_id: int | None = ...,
    ) -> UrbanObject:
        """Patch urban_object. If no parameters fiven, does nothing."""

    @abc.abstractmethod
    async def patch_object_geometry(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        object_geometry_id: int,
        geometry: shapely.geometry.base.BaseGeometry = ...,
        territory_id: int = ...,
        address: str = ...,
        osm_id: str = ...,
    ) -> ObjectGeometry:
        """Patch object_geometry. If no parameters fiven, does nothing."""

    @abc.abstractmethod
    async def get_urban_object_by_composite(
        self, physical_object_id: int, object_geometry_id: int, service_id: int | None
    ) -> UrbanObject | None:
        """Get urban object by physical_object_id, object_geometry_id and optional service_id."""

    @abc.abstractmethod
    async def get_physical_object_geometries(self, physical_object_id: int) -> gpd.GeoDataFrame:
        """Return geometries of a given physical object."""

    @abc.abstractmethod
    async def get_physical_object_services(
        self, physical_object_id: int, service_type_id: int | None = None
    ) -> list[Service]:
        """Return services of a given physical object."""

    @abc.abstractmethod
    async def get_physical_object_types(self) -> list[PhysicalObjectType]:
        """Get a list of physical object types."""

    @abc.abstractmethod
    async def upload_physical_object(self, physycal_object: PostPhysicalObject) -> UrbanObject:
        """Upload building with given geometry."""

    @abc.abstractmethod
    async def add_living_building(
        self, physical_object_id: int, residents_number: int, living_area: float, properties: dict[str, Any]
    ) -> PhysicalObject:
        """Add living building to a given physical object
        (which is supposed to have physical object type of living building).
        """

    @abc.abstractmethod
    async def get_service_types(self) -> list[ServiceType]:
        """Get a list of service types."""

    @abc.abstractmethod
    async def upload_service(self, service: PostService) -> Service:
        """Upload building with given geometry."""

    @abc.abstractmethod
    async def get_inner_territories(self, territory_id: int | None) -> list[TerritoryWithoutGeometry]:
        """Get a list of territories inside a given territory on the next level. Pass None to get top-level territory"""

    @abc.abstractmethod
    async def get_common_territory_id(self, geom: shapely.geometry.base.BaseGeometry) -> int | None:
        """Get the most deep territory id which fully covers given geometry."""

    @abc.abstractmethod
    async def get_functional_zone_types(self) -> list[FunctionalZoneType]:
        """Get a list of functional zone types."""

    @abc.abstractmethod
    async def get_functional_zones(
        self,
        territory_id: int,
        year: int,
        source: str,
        functional_zone_type_id: int | None = None,
    ) -> list[FunctionalZone]:
        """Get a list of functional zones for a territory."""

    @abc.abstractmethod
    async def upload_functional_zone(self, functional_zone: PostFunctionalZone) -> list[FunctionalZone]:
        """Add given functional zone."""
