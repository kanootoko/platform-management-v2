"""Duty scripts configuration is performed here."""

import importlib
from pathlib import Path

from .._main import Config, main, pass_config


@main.group("duty")
def duty_group():
    """Duty operations (used to solve a concrete task)."""


for file in Path(__file__).resolve().parent.glob("*.py"):
    if file.name != "__init__.py":
        importlib.import_module(f"pmv2.cli.duty.{file.name[:-3]}")
