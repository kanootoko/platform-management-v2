"""analyze command locic is located here."""

import asyncio
import itertools
import math
from typing import Any, Awaitable, Callable
import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import structlog

from pmv2.logic.utils import transform_geometry_4326_to_3857
from pmv2.urban_client._abstract import UrbanClient
from pmv2.urban_client.exceptions import ObjectNotFound


class UrbanObjectsIntersectionMatcher:
    """Matcher which checks intersection with other urban_objects with the same type of physical_object geometry."""

    def __init__(
        self,
        urban_client: UrbanClient,
        min_intersection_area_to_object: float = 0.7,
        min_intersection_area_to_geometry: float = 0.0,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        self.min_intersection_area_to_object = min_intersection_area_to_object
        self.min_intersection_area_to_geometry = min_intersection_area_to_geometry
        if logger is ...:
            self._logger = structlog.get_logger("urban_objects_intersection_matcher")
        else:
            self._logger = logger

    async def find_alternative_geometries(
        self, urban_object_ids, parallel_workers: int = 1
    ) -> tuple[dict[int, int], list[int] | None]:
        """Call `find_alternative_geometry_id` for all of the given urban_objects returning tuple of (dictionary of
        urban_object_id -> alternative geometry_id and urban objects identifiers which caused an error)
        using given number of parallel workers.
        """
        counter = 0

        def logging_wrapper(func: Awaitable[Callable[..., Any]]):
            async def wrapped(*args, **kwargs) -> Any:
                nonlocal counter
                counter += 1
                await self._logger.adebug(
                    "preparing to find alternative geometry", current=counter, total=len(urban_object_ids)
                )
                return await func(*args, **kwargs)

            return wrapped

        part_size = math.ceil(len(urban_object_ids) / parallel_workers)
        parts = [urban_object_ids[i : i + part_size] for i in range(0, len(urban_object_ids), part_size)]
        workers = [
            self._find_alternative_geometries_batch(part, logging_wrapper(self.find_alternative_geometry_id))
            for part in parts
        ]

        results = await asyncio.gather(*workers)
        resulting_dict = {}
        all_errors = []
        for matched, errors in results:
            resulting_dict.update(matched)
            if errors is not None:
                all_errors.extend(errors)
        if len(all_errors) == 0:
            all_errors = None
        return resulting_dict, all_errors

    async def _find_alternative_geometries_batch(
        self, urban_object_ids: list[int], search_func: Awaitable[Callable[[int], int]] = ...
    ) -> tuple[dict[int, int], list[int] | None]:
        if search_func is ...:
            search_func = self.find_alternative_geometry_id
        alternative_geometries: dict[int, int] = {}
        errors: list[int] = []
        for uo_id in urban_object_ids:
            try:
                alt_geometry_id = await search_func(uo_id)
            except ObjectNotFound:
                self._logger.warning("urban object not found", urban_object_id=uo_id)
                continue
            except Exception:  # pylint: disable=broad-except
                errors.append(uo_id)
                continue
            if alt_geometry_id is not None:
                alternative_geometries[uo_id] = alt_geometry_id
        if len(errors) == 0:
            errors = None
        return alternative_geometries, errors

    async def find_alternative_geometry_id(self, urban_object_id: int) -> int | None:
        """Check for intersections with other urban_objects with the same type of physical_object
        returning identifier of geometry object if found.

        Parameters:
            urban_object_id: int: identifier of urban_object to check intersections
            min_intersection_area_to_object: float: minimal value of intersection area divided by object area \
                (used only when both geometries have area)
            min_intersection_area_to_geometry: float: minimal value of intersection area divided by \
                other object geometry area (used only when both geometries have area)
        """
        urban_object = await self._urban_client.get_urban_object(urban_object_id)
        if urban_object is None:
            raise ObjectNotFound()
        object_geometry = shapely.from_wkt(urban_object.object_geometry.geometry.wkt)

        objects_around = await self._urban_client.get_objects_around(
            urban_object.object_geometry.geometry,
            urban_object.physical_object.physical_object_type.physical_object_type_id,
        )
        # better, but field is missing for now
        objects_around: gpd.GeoDataFrame = objects_around[
            objects_around["object_geometry_id"] != urban_object.object_geometry.object_geometry_id
        ]
        # objects_around: gpd.GeoDataFrame = objects_around[
        #     objects_around["physical_object_id"] != urban_object.physical_object.physical_object_id
        # ]

        if objects_around.shape[0] == 0:
            return None

        intersections = _get_intersections(
            object_geometry,
            objects_around,
            self.min_intersection_area_to_object,
            self.min_intersection_area_to_geometry,
        )

        if intersections.shape[0] == 0:
            return None
        return int(intersections.iloc[0]["object_geometry_id"])


def _get_intersections(
    geometry: shapely.geometry.base.BaseGeometry,
    around: gpd.GeoDataFrame,
    min_intersection_area_to_object: float,
    min_intersection_area_to_geometry: float,
) -> gpd.GeoDataFrame:
    og_3857 = transform_geometry_4326_to_3857(geometry)
    around_3857: gpd.GeoDataFrame = around.to_crs(3857)

    around_3857 = around_3857[around_3857.area > 0]
    around_3857 = around_3857[around_3857.intersects(og_3857) | around_3857.contains(og_3857)]

    if og_3857.geom_type == "Point":
        return around.loc[around_3857.index]

    intersection_area: pd.Series = around_3857.intersection(og_3857).area
    around_3857["intersection_object"] = np.minimum(
        intersection_area / og_3857.area, intersection_area / around_3857.area
    )
    around_3857["intersection_geometry"] = np.minimum(
        intersection_area / og_3857.area, intersection_area / around_3857.area
    )
    around_3857["intersection"] = around_3857["intersection_object"] + around_3857["intersection_geometry"]
    around_3857 = around_3857[
        (around_3857["intersection_object"] >= min_intersection_area_to_object)
        & (around_3857["intersection_geometry"] >= min_intersection_area_to_geometry)
    ].sort_values("intersection")

    return around.loc[around_3857.index]
