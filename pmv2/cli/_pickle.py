"""Physical objects uploading commands are defined here."""

import json
import pickle
import sys
from pathlib import Path

import click

from pmv2.logic.pickle import print_upto_level

from ._main import main


@main.group("pickle")
def pickles_group():
    """Operation with log-pickles (deprecated)."""


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
def preview(max_level: int, array_elements: int, dict_elements: int, pickle_file: Path):
    """Preview pickle file content.

    Single argument is a path to pickle file produced by other operation.
    """
    with pickle_file.open("rb") as file:
        content = pickle.load(file)
    print_upto_level(content, max_level, array_elements=array_elements, dict_elements=dict_elements)


@pickles_group.command("export-errors")
@click.option(
    "--output",
    "-o",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    required=True,
    help="Output path for errors objects file",
)
@click.argument("pickle_file", type=click.Path(dir_okay=False, path_type=Path))
def export_errors(output_file: Path, pickle_file: Path):
    """Export errors section from pickle file after single file upload."""
    with pickle_file.open("rb") as file:
        content = pickle.load(file)
    if "errors" not in content:
        print("File does not contain 'errors' section!")
        sys.exit(1)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(content["errors"], file, ensure_ascii=False, indent=4)


@pickles_group.command("export-errors-bulk")
@click.option(
    "--output",
    "-o",
    "output_dir",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    required=True,
    help="Output path for errors objects directory",
)
@click.argument("pickle_file", type=click.Path(dir_okay=False, path_type=Path))
def export_errors_bulk(output_dir: Path, pickle_file: Path):
    """Export errors section from pickle file after bulk upload.

    Creates multiple geojson files in the given directory.
    """
    with pickle_file.open("rb") as file:
        content = pickle.load(file)
    output_dir.mkdir(parents=True, exist_ok=True)
    if "errors" not in content:
        print("File does not contain 'errors' section!")
        sys.exit(1)

    filename: str
    for filename, errors in content["errors"].items():
        output_file = output_dir / filename
        with output_file.open("w", encoding="utf-8") as file:
            json.dump(errors, file, ensure_ascii=False, indent=4)
