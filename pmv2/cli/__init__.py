"""Click configuration is performed here."""

from ._main import main
from . import _list
from . import _upload_services
from . import _upload_physical_objects

__all__ = [
    "main",
]
