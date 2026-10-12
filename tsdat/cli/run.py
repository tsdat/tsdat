"""Commands for running pipelines from a pipeline repository."""

import logging
import re
import sys
from contextlib import contextmanager
from datetime import date as calendar_date
from datetime import time as clock_time
from pathlib import Path
from typing import Annotated

import typer

from tsdat import PipelineConfig, TransformationPipeline

from .registry import PipelineRegistry

logger = logging.getLogger(__name__)


@contextmanager
def _project_imports():
    """Allow pipeline modules in the current repository to be imported by name."""
    project_root = str(Path.cwd())
    sys.path.insert(0, project_root)
    try:
        yield
    finally:
        sys.path.remove(project_root)


def _date_string(date: str) -> str:
    """Accept YYYYMMDD or YYYYMMDD.hhmmss, returning the full timestamp."""
    if len(date) == 8:
        date += ".000000"
    try:
        if not re.fullmatch(r"\d{8}\.\d{6}", date):
            raise ValueError(date)
        calendar_date(int(date[:4]), int(date[4:6]), int(date[6:8]))
        clock_time(int(date[9:11]), int(date[11:13]), int(date[13:15]))
    except ValueError as exc:
        raise typer.BadParameter(
            "Use a valid date in YYYYMMDD[.hhmmss] format"
        ) from exc
    return date


def ingest(
    filepaths: Annotated[
        list[Path],
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            resolve_path=True,
            help="Path(s) to the input files to process.",
        ),
    ],
    clump: bool = typer.Option(
        False,
        help="Run each matching pipeline once with all of its matching input files, "
        "instead of once per file. Each pipeline group is processed separately.",
    ),
    multidispatch: bool = typer.Option(
        False,
        help="Allow an input file to match multiple pipelines; with --clump it is "
        "included in each matching pipeline's group.",
    ),
    verbose: bool = typer.Option(False, help="Turn logging level up to DEBUG."),
) -> None:
    """Run ingest pipelines selected by input file triggers in this repository."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    try:
        with _project_imports():
            _, failures, _ = PipelineRegistry().dispatch(
                [str(path) for path in filepaths],
                clump=clump,
                multidispatch=multidispatch,
            )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if failures:
        raise typer.Exit(code=1)


def vap(
    config_path: Annotated[
        Path,
        typer.Argument(
            exists=True, dir_okay=False, readable=True, help="VAP pipeline config file."
        ),
    ],
    start: str = typer.Option(
        ...,
        "--begin",
        "-b",
        help="Begin date in YYYYMMDD[.hhmmss] format.",
        callback=_date_string,
    ),
    end: str = typer.Option(
        ...,
        "--end",
        "-e",
        help="End date in YYYYMMDD[.hhmmss] format.",
        callback=_date_string,
    ),
    verbose: bool = typer.Option(False, help="Turn logging level up to DEBUG."),
) -> None:
    """Run a transformation pipeline for a specified time interval."""
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)
    with _project_imports():
        pipeline = PipelineConfig.from_yaml(config_path).instantiate_pipeline()
        if not isinstance(pipeline, TransformationPipeline):
            raise typer.BadParameter(
                f"Invalid pipeline class selected: '{pipeline.__repr_name__()}', "
                "expected a TransformationPipeline subclass."
            )
        try:
            pipeline.run(inputs=[start, end])
        except Exception:
            logger.exception(
                "Pipeline '%s' failed to process input: %s",
                pipeline.__repr_name__(),
                [start, end],
            )
            logger.info(
                "Processing completed with 0 successes, 1 failures, and 0 skipped."
            )
            raise typer.Exit(code=1) from None
    logger.info("Processing completed with 1 successes, 0 failures, and 0 skipped.")
