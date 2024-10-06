"""Entities listing commands is defined here."""

import asyncio
import json
from typing import Literal

import click

from pmv2.logic import list_territories as territories_logic

from ._main import Config, main, pass_config


@main.command("list-territories")
@pass_config
@click.option(
    "--max-level",
    "-l",
    type=int,
    help="Maximum level of territories printed",
)
def list_territories(
    config: Config,
    max_level: int | None,
):
    """List territories available in Urban API in hierarchy format."""
    urban_client = config.urban_client
    territories = asyncio.run(territories_logic.get_territories(urban_client, max_level))
    if len(territories) == 0:
        print("There are no territories available")
        return
    territories_logic.print_terrirories(territories)


@main.command("list-service-types")
@pass_config
@click.option(
    "--format",
    "-f",
    type=click.Choice(["pretty", "json"], case_sensitive=False),
    default="pretty",
    show_default=True,
    help="Format of data output",
)
@click.option(
    "--order-by",
    "-s",
    type=click.Choice(["id", "name"], case_sensitive=False),
    default="id",
    show_default=True,
    help="Attribute to sort by",
)
def list_service_types(
    config: Config,
    format: Literal["pretty", "json"],  # pylint: disable=redefined-builtin
    order_by: Literal["id", "name"],
):
    """List service types available in Urban API."""
    urban_client = config.urban_client
    service_types = asyncio.run(urban_client.get_service_types())
    if len(service_types) == 0:
        print("There are no service_types available")
        return
    if order_by == "id":
        service_types.sort(key=lambda el: el.service_type_id)
    else:
        service_types.sort(key=lambda el: el.name)
    if format == "pretty":
        for service_type in service_types:
            print(f"{service_type.service_type_id:3} - {service_type.name}")
    else:
        print(json.dumps({"service_types": [st.model_dump() for st in service_types]}))


@main.command("list-physical-object-types")
@pass_config
@click.option(
    "--format",
    "-f",
    type=click.Choice(["pretty", "json"], case_sensitive=False),
    default="pretty",
    show_default=True,
    help="Format of data output",
)
@click.option(
    "--order-by",
    "-s",
    type=click.Choice(["id", "name"], case_sensitive=False),
    default="id",
    show_default=True,
    help="Attribute to sort by",
)
def list_physical_object_types(
    config: Config,
    format: Literal["pretty", "json"],  # pylint: disable=redefined-builtin
    order_by: Literal["id", "name"],
):
    """List physical_object types available in Urban API."""
    urban_client = config.urban_client
    physical_object_types = asyncio.run(urban_client.get_physical_object_types())
    if len(physical_object_types) == 0:
        print("There are no physical_object_types available")
        return
    if order_by == "id":
        physical_object_types.sort(key=lambda el: el.physical_object_type_id)
    else:
        physical_object_types.sort(key=lambda el: el.name)

    if format == "pretty":
        for service_type in physical_object_types:
            print(f"{service_type.physical_object_type_id:3} - {service_type.name}")
    else:
        print(json.dumps({"physical_object_types": [st.model_dump() for st in physical_object_types]}))
