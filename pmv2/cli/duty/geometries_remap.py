"""Gometry objects remapping to a correct territory is refined here."""

import asyncio
import datetime
import pickle
import sys
import time
from pathlib import Path
from typing import Any

import click
import pandas as pd

from pmv2.logic.duty.geometries_remap import GeometryObjectsTerritoryMapper


from . import Config, duty_group, pass_config


@duty_group.command("remap-geometry-objects")
@pass_config
@click.option(
    "--input-file",
    "-i",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to CSV with single object_geometry_id column",
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
def remap_object_geometries(
    config: Config,
    *,
    input_file: Path,
    parallel_workers: int,
    output_pickle: Path | None,
):
    """Search for the correct geometry for the input object_geometries by identifiers and patch geometry object to
    fix their parent_id if it differs from found one.
    """
    if output_pickle is None:
        output_pickle = Path(f"remap_object_geometries_{int(time.time())}.pickle")
    if output_pickle.is_dir():
        output_pickle = output_pickle / f"remap_object_geometries_{int(time.time())}.pickle"
    urban_client = config.urban_client
    if not asyncio.run(urban_client.is_alive()):
        print("Urban API at is unavailable, exiting")
        sys.exit(1)

    results: dict[str, Any] = {
        "type": "remap_object_geometries",
        "time_start": datetime.datetime.now(),
        "input_file": str(input_file.resolve()),
    }

    df: pd.DataFrame = pd.read_csv(input_file)
    df = df.drop_duplicates()
    print(f"Read file {input_file.name} - {df.shape[0]} objects after filtering")
    try:
        object_geometry_ids = list(map(int, df.iloc[:, 0]))
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Could not exctact object_geometry_id integers data: {exc!r}")

    mapper = GeometryObjectsTerritoryMapper(config.urban_client, logger=config.logger)

    remapped, errors = asyncio.run(
        mapper.remap_object_geometries_to_territories(object_geometry_ids, parallel_workers=parallel_workers)
    )

    results["mapped"] = remapped
    results["errors"] = errors
    results["metadata"] = {"total": len(object_geometry_ids), "remapped": len(remapped)}
    config.logger.info("Finished", log_filename=output_pickle.name)
    results["time_finish"] = datetime.datetime.now()

    with open(output_pickle, "wb") as file:
        pickle.dump(results, file)
