"""Abstract protocol for Urban API client is defined here."""

import abc
from typing import Any

import geopandas as gpd
import shapely

from pmv2.urban_client.models import (
    FunctionalZone,
    FunctionalZoneType,
    LivingBuilding,
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
    async def get_urban_object(
        self, physical_object_id: int, object_geometry_id: int, service_id: int | None
    ) -> UrbanObject | None:
        """Get urban object by physical_object_id, object_geometry_id and optional service_id."""

    @abc.abstractmethod
    async def get_physical_object_geometries(self, physical_object_id: int) -> gpd.GeoDataFrame:
        """Return geometries of a given physical object."""

    @abc.abstractmethod
    async def get_physical_object_types(self) -> list[PhysicalObjectType]:
        """Get a list of physical object types."""

    @abc.abstractmethod
    async def upload_physical_object(self, physycal_object: PostPhysicalObject) -> UrbanObject:
        """Upload building with given geometry."""

    @abc.abstractmethod
    async def add_living_building(
        self, physical_object_id: int, residents_number: int, living_area: float, properties: dict[str, Any]
    ) -> LivingBuilding:
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
        self, territory_id: int, functional_zone_type_id: int | None = None, include_child_territories: bool = True
    ) -> list[FunctionalZone]:
        """Get a list of functional zones for a territory."""

    @abc.abstractmethod
    async def upload_functional_zone(self, functional_zone: PostFunctionalZone) -> list[FunctionalZone]:
        """Add given functional zone."""
