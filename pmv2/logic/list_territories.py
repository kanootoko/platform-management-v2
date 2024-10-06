"""list-territories logic is located here."""

from dataclasses import dataclass, field
from pmv2.urban_client import UrbanClient
from pmv2.urban_client.models import TerritoryWithoutGeometry


@dataclass
class TerritoryInfo:
    """Territory info to return for command."""

    territory: TerritoryWithoutGeometry
    inner: list["TerritoryInfo"] = field(default_factory=list)


async def get_territories(urban_client: UrbanClient, max_level: int) -> list[TerritoryInfo]:
    """Get territories list in hierarchy form."""
    return await _get_inner(urban_client, None, max_level)

def print_terrirories(territories: list[TerritoryInfo], indent: int = 2) -> None:
    """Print territories list in hierarchy form."""
    for t in territories:
        print(
            f"{' ' * (indent) * (t.territory.level-1)}{t.territory.territory_id:5}"
            f" {'>' * t.territory.level} {t.territory.name}"
        )
        print_terrirories(t.inner)


async def _get_inner(urban_client: UrbanClient, parent_id: int | None, max_level: int | None) -> list[TerritoryInfo]:
    res = await urban_client.get_inner_territories(parent_id)
    res.sort(key=lambda el: el.territory_id)
    result: list[TerritoryInfo] = []
    for territory in res:
        result.append(TerritoryInfo(territory))
        if max_level is None or max_level > territory.level:
            result[-1].inner = await _get_inner(urban_client, territory.territory_id, max_level)
    return result
