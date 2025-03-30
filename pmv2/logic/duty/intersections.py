"""urban-objects-intersections command locic is located here."""

import asyncio
import math
from typing import Awaitable, Callable

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import structlog

from pmv2.logic.utils import logging_wrapper, transform_geometry_4326_to_3857
from pmv2.urban_client._abstract import UrbanClient
from pmv2.urban_client.exceptions import ObjectNotFoundError


class UrbanObjectsIntersectionMatcher:
    """Matcher which checks intersection with other urban_objects with the same type of physical_object geometry."""

    def __init__(
        self,
        urban_client: UrbanClient,
        min_intersection_area_to_object: float = 0.7,
        min_intersection_area_to_geometry: float = 0.0,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        """Initialize matcher.
        
        Parameters:
            urban_client: UrbanClient: urban client to perform operations
            min_intersection_area_to_object: float: minimal value of intersection area divided by object area \
                (used only when both geometries have area)
            min_intersection_area_to_geometry: float: minimal value of intersection area divided by \
                other object geometry area (used only when both geometries have area)
            logger: BoundLogger: structlog logger. If not given, default with name 'urban_objects_intersection_matcher'
                will be used
        """
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
        """Try to find an alternative geometry_id for all of the given urban_objects returning tuple of (dictionary of
        urban_object_id -> alternative geometry_id and urban objects identifiers which caused an error)
        using given number of parallel workers.
        """
        part_size = math.ceil(len(urban_object_ids) / parallel_workers)
        parts = [urban_object_ids[i : i + part_size] for i in range(0, len(urban_object_ids), part_size)]
        workers = [
            self._find_alternative_geometries_batch(
                part,
                logging_wrapper(
                    self._logger,
                    len(urban_object_ids),
                    "preparing to find alternative geometry",
                    self.find_alternative_geometry_id,
                ),
            )
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

    async def find_alternative_geometry_id(self, urban_object_id: int) -> int | None:
        """Check for intersections with other urban_objects with the same type of physical_object
        returning identifier of geometry object if found.
        """
        urban_object = await self._urban_client.get_urban_object(urban_object_id)
        if urban_object is None:
            raise ObjectNotFoundError()
        object_geometry = shapely.from_wkt(urban_object.object_geometry.geometry.wkt)

        objects_around = await self._urban_client.get_objects_around(
            urban_object.object_geometry.geometry,
            urban_object.physical_object.physical_object_type.physical_object_type_id,
        )
        objects_around: gpd.GeoDataFrame = objects_around[
            objects_around["object_geometry_id"] != urban_object.object_geometry.object_geometry_id
        ]

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

    async def update_geometry_ids(
        self, uo_geoms: dict[int, int], parallel_workers: int = 1
    ) -> tuple[list[int], list[int] | None]:
        """Patch urban objects given by id to replace their object_geometry_id with given value"""

        part_size = math.ceil(len(uo_geoms) / parallel_workers)
        dict_as_list = list(uo_geoms.items())
        parts = [dict(dict_as_list[i : i + part_size]) for i in range(0, len(uo_geoms), part_size)]
        workers = [
            self._update_geometry_ids_batch(
                part,
                logging_wrapper(
                    self._logger,
                    len(uo_geoms),
                    "preparing to update urban_object's object_geometry_id",
                    self.update_urban_object_geometry_id,
                ),
            )
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

    async def update_urban_object_geometry_id(self, urban_object_id: int, object_geometry_id: int) -> bool | None:
        """Get current urban objects geometry id and update it if needed.

        Return None if given urban object does not exist, True if update was performed, False otherwise."""
        uo = await self._urban_client.get_urban_object(urban_object_id)

        if uo is None:
            return None
        if uo.object_geometry.object_geometry_id == object_geometry_id:
            return False
        await self._logger.adebug(
            "updating object_geometry_id",
            old_object_geometry_id=uo.object_geometry.object_geometry_id,
            new_object_geometry_id=object_geometry_id,
        )
        await self._urban_client.patch_urban_object(urban_object_id, object_geometry_id=object_geometry_id)
        return True

    async def _find_alternative_geometries_batch(
        self, urban_object_ids: list[int], search_func: Awaitable[Callable[[int], int]] = ...
    ) -> tuple[dict[int, int], list[int] | None]:
        if search_func is ...:
            search_func = self.find_alternative_geometry_id
        alternative_geometries: dict[int, int] = {}
        errors: list[int] = []
        for uo_id in urban_object_ids:
            try:
                alt_geometry_id = await search_func(
                    uo_id,
                )
            except ObjectNotFoundError:
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

    async def _update_geometry_ids_batch(
        self, uo_geoms: dict[int, int], update_func: Awaitable[Callable[[int], int]] = ...
    ) -> tuple[dict[int, int], list[int] | None]:
        if update_func is ...:
            update_func = self.update_urban_object_geometry_id
        updated: list[int] = []
        errors: list[int] = []
        for uo_id, og_id in uo_geoms.items():
            try:
                alt_geometry_id = await update_func(uo_id, og_id)
            except ObjectNotFoundError:
                self._logger.warning("urban object not found", urban_object_id=uo_id)
                continue
            except Exception:  # pylint: disable=broad-except
                errors.append(uo_id)
                continue
            if alt_geometry_id is not None:
                updated.append(uo_id)
        if len(errors) == 0:
            errors = None
        return updated, errors


def _get_intersections(
    geometry: shapely.geometry.base.BaseGeometry,
    around: gpd.GeoDataFrame,
    min_intersection_area_to_object: float,
    min_intersection_area_to_geometry: float,
) -> gpd.GeoDataFrame:
    og_3857 = transform_geometry_4326_to_3857(geometry)
    around_3857: gpd.GeoDataFrame = around.to_crs(3857)

    around_3857 = around_3857[around_3857.area > 0]

    if og_3857.geom_type == "Point":
        if not any(around_3857.contains(og_3857)):
            around_3857["geometry"] = around_3857.buffer(5)
        around_3857 = around_3857[around_3857.contains(og_3857)]
        return around.loc[around_3857.index]

    around_3857 = around_3857[around_3857.intersects(og_3857) | around_3857.contains(og_3857)]

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
