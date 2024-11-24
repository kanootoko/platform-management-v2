"""Physical objects uploading commands are defined here."""

import pickle
from pathlib import Path

import click

from pmv2.logic.pickle import print_upto_level

from ._main import main


@main.group("pickle")
def pickles_group():
    """Operation with log-pickles."""


@pickles_group.command("preview")
@click.argument("pickle_file", type=click.Path(dir_okay=False, path_type=Path))
@click.option(
    "--max-level",
    "-l",
    envvar="MAX_LEVEL",
    type=int,
    default=3,
    show_default=True,
    show_envvar=True,
    help="Max level to print values recursively",
)
@click.option(
    "--array-elements",
    envvar="ARRAY_ELEMENTS",
    type=int,
    default=2,
    show_default=True,
    show_envvar=True,
    help="Number of array elements to print",
)
@click.option(
    "--dict-elements",
    envvar="DICT_ELEMENTS",
    type=int,
    default=5,
    show_default=True,
    show_envvar=True,
    help="Number of dictionaries elements to print",
)
def prepare_bulk_config(max_level: int, array_elements: int, dict_elements: int, pickle_file: Path):
    """Preview pickle file content.

    Single argument is a path to pickle file produced by other operation.
    """
    with pickle_file.open("rb") as file:
        content = pickle.load(file)
    print_upto_level(content, max_level, array_elements=array_elements, dict_elements=dict_elements)
