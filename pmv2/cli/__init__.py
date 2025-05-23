"""Click configuration is performed here."""

import importlib
from pathlib import Path

from ._main import main

for file in Path(__file__).resolve().parent.glob("*.py"):
    if file.name != "__init__.py":
        importlib.import_module(f"pmv2.cli.{file.name[:-3]}")

for file in Path(__file__).resolve().parent.glob("*/__init__.py"):
    module = file.resolve().parent
    importlib.import_module(f"pmv2.cli.{module.name}")

__all__ = [
    "main",
]
