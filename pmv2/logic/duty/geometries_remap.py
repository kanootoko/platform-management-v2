"""analyze command locic is located here."""

import asyncio
import math
from typing import Any, Awaitable, Callable
import structlog

from pmv2.urban_client._abstract import UrbanClient
from pmv2.urban_client.exceptions import ObjectNotFoundError


class GeometryObjectsTerritoryMapper:
    """Mapper which searches for a correct territory for a given object_geometries."""

    def __init__(
        self,
        urban_client: UrbanClient,
        logger: structlog.stdlib.BoundLogger = ...,
    ):
        self._urban_client = urban_client
        if logger is ...:
            self._logger = structlog.get_logger("object_geometries_territories_mapper")
        else:
            self._logger = logger

    async def remap_object_geometries_to_territories(
        self, object_geometry_ids, parallel_workers: int = 1
    ) -> tuple[dict[int, int], list[int] | None]:
        """Try to find a correct territory for each of the given object_geometries
        using given number of parallel workers.
        """
        counter = 0

        def logging_wrapper(func: Awaitable[Callable[..., Any]]):
            async def wrapped(*args, **kwargs) -> Any:
                nonlocal counter
                counter += 1
                await self._logger.adebug(
                    "preparing to remap object_geometry", current=counter, total=len(object_geometry_ids)
                )
                try:
                    return await func(*args, **kwargs)
                except Exception as exc:
                    await self._logger.aexception("error on remapping", current=counter)

            return wrapped

        part_size = math.ceil(len(object_geometry_ids) / parallel_workers)
        parts = [object_geometry_ids[i : i + part_size] for i in range(0, len(object_geometry_ids), part_size)]
        workers = [
            self._remap_object_geometries_to_territories_batch(
                part, logging_wrapper(self.remap_object_geometry_territory)
            )
            for part in parts
        ]

        results = await asyncio.gather(*workers)
        resulting_dict = {}
        all_errors = []
        for remapped, errors in results:
            resulting_dict.update(remapped)
            if errors is not None:
                all_errors.extend(errors)
        if len(all_errors) == 0:
            all_errors = None
        return resulting_dict, all_errors

    async def remap_object_geometry_territory(self, object_geometry_id: int) -> int | None:
        """Search for a correct territory_id for a given territory.

        Return None if no changes were performed, or integer value - new territory_id if object has been updated.

        Raise ObjectNotFoundError if there is no object_geometry with given identifier,
        or TerritoryNotFoundError if no territory is found for the geometry at all.
        """
        object_geometry = await self._urban_client.get_object_geometry(object_geometry_id)
        if object_geometry is None:
            raise ObjectNotFoundError()

        new_territory_id = await self._urban_client.get_common_territory_id(object_geometry.geometry)

        if new_territory_id is None:
            raise TerritoryNotFoundError()

        if new_territory_id == object_geometry.territory.id:
            return None

        await self._urban_client.patch_object_geometry(object_geometry.object_geometry_id, territory_id=new_territory_id)

        return new_territory_id

    async def _remap_object_geometries_to_territories_batch(
        self, urban_object_ids: list[int], remap_func: Awaitable[Callable[[int], int]] = ...
    ) -> tuple[dict[int, int], list[int] | None]:
        if remap_func is ...:
            remap_func = self.remap_object_geometry_territory
        alternative_geometries: dict[int, int] = {}
        errors: list[int] = []
        for uo_id in urban_object_ids:
            try:
                alt_geometry_id = await remap_func(
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


class TerritoryNotFoundError(RuntimeError):
    """Impossible to find a corresponding territory."""
