"""Common utilities are located here."""

import pyproj
import shapely
import shapely.ops

_crs_transformer = pyproj.Transformer.from_crs(4326, 3857, always_xy=True)


def transform_geometry_4326_to_3857(geometry: shapely.geometry.base.BaseGeometry) -> shapely.geometry.base.BaseGeometry:
    """Return shapely geometry transformed from 4326 toi 3857 crs."""
    return shapely.ops.transform(_crs_transformer.transform, geometry)
