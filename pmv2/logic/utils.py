"""Common utilities are located here."""

import asyncio
import json
from pathlib import Path
from typing import Any, Awaitable, Callable

import geopandas as gpd
import pyproj
import shapely
import shapely.ops
import structlog

from pmv2.urban_client.exceptions import APIConnectionError
from pmv2.urban_client.http.exceptions import InvalidStatusCode

_crs_transformer = pyproj.Transformer.from_crs(4326, 3857, always_xy=True)


class AlreadyLoggedException(RuntimeError):
    """Exception which was already logged before and does not need to print stack trace again."""


def transform_geometry_4326_to_3857(geometry: shapely.geometry.base.BaseGeometry) -> shapely.geometry.base.BaseGeometry:
    """Return shapely geometry transformed from 4326 toi 3857 crs."""
    return shapely.ops.transform(_crs_transformer.transform, geometry)


def try_load_json(value: Any) -> Any:
    """Try to read a value as json if it is a string, otherwise return as-is."""
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:  # pylint: disable=broad-except
        return value


def read_geojson(path: Path) -> gpd.GeoDataFrame:
    """Read GeoJSON from file, drop duplicates and null geometries, ensure that CRS is 4326 and try to load columns to
    json values.
    """
    gdf: gpd.GeoDataFrame = gpd.read_file(path)
    gdf = gdf.drop_duplicates().dropna(subset="geometry").to_crs(4326)

    for column in gdf.columns:
        gdf[column] = gdf[column].apply(try_load_json)
    return gdf


def logging_wrapper(
    logger: structlog.stdlib.BoundLogger,
    total: int,
    text: str,
    func: Awaitable[Callable[..., Any]],
    max_attempts: int | None = None,
):
    """Return an async function wrapper for async `func` which will print info message with given `text` and `total`
    before each call increasing counter, retry `InvalidStatusCode` and `APIConnectionError` exceptions with
    counting attempts until it reaches `max_attempts` if set."""
    counter = 0
    errors = 0
    success = 0

    async def wrapped(*args, **kwargs):
        nonlocal counter, errors, success
        counter += 1
        await logger.adebug(text, current=counter, success=success, total=total, errors=errors)
        attempt = 0
        while True:
            attempt += 1
            try:
                res = await func(*args, **kwargs)
                success += 1
                return res
            except (InvalidStatusCode, APIConnectionError) as exc:
                if isinstance(exc, InvalidStatusCode) and "504" not in str(exc):
                    raise
                if max_attempts is not None and attempt > max_attempts:
                    errors += 1
                    raise

                await logger.awarning(
                    "Suppressing urban_api error, sleeping for 5 seconds", error_type=type(exc), attempt=attempt
                )
                await asyncio.sleep(5)
            except Exception:
                errors += 1
                raise

    return wrapped


def try_float(val: Any) -> float | None:
    """Try to cast value to float."""
    if val is None:
        return None
    try:
        if isinstance(val, str):
            return float(val.replace(",", "."))
        return float(val)
    except ValueError:
        return None


def try_int(val: Any) -> int | None:
    """Try to cast value to integer."""
    float_val = try_float(val)
    if float_val is None:
        return None
    return int(float_val)


def try_str(val: Any) -> str | None:
    """Try to cast value to string."""
    if val is None:
        return None
    if isinstance(val, str):
        return val
    return str(val)
