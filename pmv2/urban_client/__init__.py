"""Urban_api client is located here. There is a possibility it will move to an individual package."""

import structlog.stdlib

from ._abstract import UrbanClient
from .http import HTTPUrbanClient

__all__ = [
    "UrbanClient",
    "make_http_client",
]


def make_http_client(host: str, logger: structlog.stdlib.BoundLogger = ...) -> UrbanClient:
    """Get HTTP Urban API client."""
    return HTTPUrbanClient(host, logger)
