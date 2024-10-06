"""HTTP-specific datatypes are defined here."""

from typing import Generic, TypeVar

from aiohttp import ClientSession
from pydantic import BaseModel

from pmv2.urban_client.http.exceptions import InvalidStatusCode

_T = TypeVar("_T")


class Paginated(BaseModel, Generic[_T]):
    """Paginated response."""

    count: int
    prev: str | None
    next: str | None
    results: list[_T]

    async def get_all_pages(self, session: ClientSession) -> list[_T]:
        """Get all pages if there are more than one and return a whole list."""
        results: list[_T] = list(self.results)
        url = self.next
        while url is not None:
            resp = await session.get(url)
            if resp.status != 200:
                raise InvalidStatusCode(f"Expected code 200, got {resp.status}")
            result = Paginated[_T].model_validate_json(await resp.text())
            url = result.next
            results += result.results
        return results
