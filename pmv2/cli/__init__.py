"""Click configuration is performed here."""

from . import _list, _pickle, _upload_buildings, _upload_functional_zones, _upload_physical_objects, _upload_services
from ._main import main

__all__ = [
    "main",
]
