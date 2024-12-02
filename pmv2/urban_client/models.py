"""Urban API models are defined here."""

import datetime
from typing import Any

import geojson_pydantic as gjp
import shapely
from geojson_pydantic.geometries import Geometry
from pydantic import BaseModel


def shapely_to_geometry(geom: shapely.geometry.base.BaseGeometry) -> Geometry:
    """Construct geojson-pydantic Geometry object from shapely geometry."""
    cls = None
    if isinstance(geom, shapely.Point):
        cls = gjp.Point
    if isinstance(geom, shapely.Polygon):
        cls = gjp.Polygon
    if isinstance(geom, shapely.MultiPolygon):
        cls = gjp.MultiPolygon
    if isinstance(geom, shapely.LineString):
        cls = gjp.LineString
    if isinstance(geom, shapely.MultiLineString):
        cls = gjp.MultiLineString
    if isinstance(geom, shapely.MultiPoint):
        cls = gjp.MultiPoint
    if isinstance(geom, shapely.GeometryCollection):
        cls = gjp.GeometryCollection
    if cls is None:
        raise ValueError(f"Invalid input geometry type: {type(geom)}")
    return cls(**shapely.geometry.mapping(geom))


class PostPhysicalObject(BaseModel):
    """Data of a physical object to be uploaded in the Urban API."""

    geometry: Geometry
    territory_id: int
    physical_object_type_id: int
    centre_point: gjp.Point | None = None
    address: str | None = None
    name: str | None = None
    properties: dict[str, Any] | None = None

    _geometry: shapely.geometry.base.BaseGeometry | None = None

    def shapely_geometry(self) -> shapely.geometry.base.BaseGeometry:
        """Get shapely object from geometry."""
        if self._geometry is None:
            self._geometry = shapely.from_wkt(self.geometry.wkt)
        return self._geometry


class PhysicalObjectType(BaseModel):
    """Type of physical object id and name."""

    physical_object_type_id: int
    name: str


class TerritoryBase(BaseModel):
    """Territory id and name."""

    id: int
    name: str


class PhysicalObject(BaseModel):
    """Physical object entity."""

    physical_object_id: int
    physical_object_type: PhysicalObjectType
    name: str | None
    properties: dict[str, Any]
    created_at: datetime.datetime
    updated_at: datetime.datetime


class LivingBuilding(BaseModel):
    """Living building additional to physical object of corresponding type."""

    living_building_id: int
    physical_object: PhysicalObject
    residents_number: int | None = None
    living_area: float | None
    properties: dict[str, Any]


class ObjectGeometry(BaseModel):
    """Object geometry entity."""

    object_geometry_id: int
    territory: TerritoryBase
    address: str | None
    geometry: Geometry
    centre_point: gjp.Point
    created_at: datetime.datetime
    updated_at: datetime.datetime


class ServiceType(BaseModel):
    """Type of service."""

    service_type_id: int
    name: str


class TerritoryType(BaseModel):
    """Type of territory."""

    territory_type_id: int
    name: str


class Service(BaseModel):
    """Service entity."""

    service_id: int
    service_type: ServiceType
    territory_type: TerritoryType | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]
    created_at: datetime.datetime
    updated_at: datetime.datetime


class UrbanObject(BaseModel):
    """Urban object entity."""

    urban_object_id: int
    physical_object: PhysicalObject
    object_geometry: ObjectGeometry
    service: Service | None


class PostService(BaseModel):
    """Data of a service to be uploaded to Urban API."""

    physical_object_id: int
    object_geometry_id: int
    service_type_id: int
    territory_type_id: int | None
    name: str | None
    capacity_real: int | None
    properties: dict[str, Any]


class TerritoryWithoutGeometry(BaseModel):
    """Territory without geometry."""

    territory_id: int
    territory_type: TerritoryType
    parent_id: int | None
    name: str
    level: int
    properties: dict[str, Any]
    admin_center: int | None
    okato_code: str | None
    created_at: datetime.datetime
    updated_at: datetime.datetime


class FunctionalZoneType(BaseModel):
    """Functional zone type entity."""

    functional_zone_type_id: int
    name: str
    zone_nickname: str
    description: str


class FunctionalZoneTypeBase(BaseModel):
    """Functional zone type base entity with only id and name."""

    id: int
    name: str


class PostFunctionalZone(BaseModel):
    """Functional zone entity."""

    geometry: Geometry
    territory_id: int
    functional_zone_type_id: int
    name: str | None
    source: str
    year: int
    properties: dict[str, Any]


class FunctionalZone(BaseModel):
    """Functional zone entity."""

    functional_zone_id: int
    geometry: Geometry
    territory: TerritoryBase
    functional_zone_type: FunctionalZoneTypeBase
    properties: dict[str, Any]
    created_at: datetime.datetime
    updated_at: datetime.datetime
