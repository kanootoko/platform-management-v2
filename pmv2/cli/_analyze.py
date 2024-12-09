"""Buildings uploading commands are defined here."""

import asyncio
import datetime
import json
import pickle
import sys
from pathlib import Path
import time
from typing import Any

import click
import pandas as pd

from pmv2.logic.analyze import UrbanObjectsIntersectionMatcher

from ._main import Config, main, pass_config


@main.group("analyze")
def analyze_group():
    """Analyze operations."""


@analyze_group.command("urban-objects-intersections")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to CSV with single urban_object_id column",
)
@click.option(
    "--parallel-workers",
    "-w",
    type=int,
    default=1,
    help="Number of workers to upload services in parallel",
)
@click.option(
    "--output",
    "output_file",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    required=True,
    help="Output path for matched data",
)
@click.option(
    "--output-pickle",
    "output_pickle",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    show_default="analyze_<timestamp>.pickle",
    help="Output path for analized data pickle file",
)
def analyze_urban_objects_intersections(
    config: Config,
    *,
    input_file: Path,
    parallel_workers: int,
    output_file: Path,
    output_pickle: Path | None,
):
    """Search for an alternative geometry for the given urban objects.

    Useful to map points-buildings to actual uploaded buildings polygons.
    """
    if output_pickle is None:
        output_pickle = Path(f"analyze_{int(time.time())}.pickle")
    if output_pickle.is_dir():
        output_pickle = output_pickle / f"analyze_{int(time.time())}.pickle"
    urban_client = config.urban_client
    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

    results: dict[str, Any] = {
        "type": "analyze_urban_objects_intersections",
        "time_start": datetime.datetime.now(),
        "input_file": str(input_file.resolve()),
    }

    df: pd.DataFrame = pd.read_csv(input_file)
    df = df.drop_duplicates()
    print(f"Read file {input_file.name} - {df.shape[0]} objects after filtering")
    try:
        urban_object_ids = list(map(int, df.iloc[:, 0]))
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Could not exctact urban_object_id integers data: {exc!r}")

    matcher = UrbanObjectsIntersectionMatcher(config.urban_client, logger=config.logger)

    matched, errors = asyncio.run(
        matcher.find_alternative_geometries(urban_object_ids, parallel_workers=parallel_workers)
    )

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(matched, file)

    results["matched"] = matched
    results["errors"] = errors
    results["metadata"] = {"total": len(urban_object_ids), "matched": len(matched)}
    config.logger.info("Finished", log_filename=output_pickle.name)
    results["time_finish"] = datetime.datetime.now()
    with open(output_pickle, "wb") as file:
        pickle.dump(results, file)

@analyze_group.command("update-geometry-objects-ids")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to CSV with single urban_object_id column",
)
@click.option(
    "--parallel-workers",
    "-w",
    type=int,
    default=1,
    help="Number of workers to upload services in parallel",
)
@click.option(
    "--output-pickle",
    "output_pickle",
    type=click.Path(dir_okay=False, writable=True, path_type=Path),
    show_default="analyze_<timestamp>.pickle",
    help="Output path for analized data pickle file",
)
def update_geometry_objects_ids(
    config: Config,
    *,
    input_file: Path,
    parallel_workers: int,
    output_pickle: Path | None,
):
    """Update urban objects object_geometry_ids by given mapping (urban_object_id -> object_geometry_id)."""
    if output_pickle is None:
        output_pickle = Path(f"updated_og_{int(time.time())}.pickle")
    if output_pickle.is_dir():
        output_pickle = output_pickle / f"updated_og_{int(time.time())}.pickle"
    urban_client = config.urban_client
    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

    try:
        with input_file.open("r", encoding="utf-8") as file:
            to_update: dict[int, int] = dict(map(lambda kv: (int(kv[0]), kv[1]), json.load(file).items()))
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Could not load input data: {exc!r}")

    results: dict[str, Any] = {
        "type": "update_geometry_object_ids",
        "time_start": datetime.datetime.now(),
        "input_file": str(input_file.resolve()),
    }

    updater = UrbanObjectsIntersectionMatcher(config.urban_client, logger=config.logger)

    updated, errors = asyncio.run(
        updater.update_geometry_ids(to_update, parallel_workers=parallel_workers)
    )

    results["updated"] = updated
    results["errors"] = errors
    results["metadata"] = {"total": len(to_update), "matched": len(updated)}
    config.logger.info("Finished", log_filename=output_pickle.name)
    results["time_finish"] = datetime.datetime.now()
    with open(output_pickle, "wb") as file:
        pickle.dump(results, file)
