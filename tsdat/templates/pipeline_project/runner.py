import logging
from pathlib import Path
from typing import List
from tsdat import PipelineConfig, TransformationPipeline

import typer

from utils.registry import PipelineRegistry


logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False)


def pad_date_string(date: str) -> str:
    """Given a date like YYYYMMDD[.hhmmss] returns a string like YYYYMMDD.hhmmss"""
    if len(date) == len("YYYYMMDD"):
        date += ".000000"
    if len(date) != len("YYYYMMDD.hhmmss"):
        raise typer.BadParameter(f"Date string not formatted correctly: {date}")
    return date


@app.command()
def ingest(
    filepaths: List[Path] = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path(s) to the file(s) to process",
    ),
    clump: bool = typer.Option(
        False,
        help="A flag indicating if the dispatcher should use a single pipeline to "
        "process the input keys. This typically results in one output data file being "
        "produced. Omit this option to run files independently and generally produce one"
        " output data file for each input file.",
    ),
    multidispatch: bool = typer.Option(
        False,
        help="A flag indicating if the dispatcher is allowed to use multiple pipelines "
        "to process each input key. If True, any pipeline whose regex pattern matches an "
        "input key will be used to process the input key.",
    ),
    verbose: bool = typer.Option(False, help="Turn logging level up to DEBUG."),
    # pipeline: str = typer.Option() # IDEA: Ability to run a specific ingest / folder
):
    """Main entry point to the ingest controller. This script takes a path to an input
    file, automatically determines which ingest(s) to use, and runs those ingests on the
    provided input data."""

    # If in verbose mode, then turn up logging to DEBUG
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # Downstream code expects a list of strings
    files = [str(file) for file in filepaths]
    logger.debug(f"Found input files: {files}")

    # Run the pipeline on the input files
    dispatcher = PipelineRegistry()
    dispatcher.dispatch(files, clump=clump, multidispatch=multidispatch)


@app.command()
def vap(
    config_path: Path = typer.Argument(
        ...,
        exists=True,
        dir_okay=False,
        help="The path to the vap / transform pipeline config file to use",
    ),
    start: str = typer.Option(
        ...,
        "--begin",
        "-b",
        help="Begin date in 'YYYYMMDD.hhmmss' format",
        callback=pad_date_string,
    ),
    end: str = typer.Option(
        ...,
        "--end",
        "-e",
        help="End date in 'YYYYMMDD.hhmmss' format",
        callback=pad_date_string,
    ),
    verbose: bool = typer.Option(False, help="Turn logging level up to DEBUG."),
):
    successes, failures, skipped = 0, 0, 0
    logging.basicConfig(level=logging.DEBUG if verbose else logging.INFO)

    config = PipelineConfig.from_yaml(config_path)
    pipeline = config.instantiate_pipeline()

    if not isinstance(pipeline, TransformationPipeline):
        raise ValueError(
            f"Invalid pipeline class selected: '{pipeline.__repr_name__()}', expected"
            " 'TransformationPipeline' subclass."
        )

    try:
        pipeline.run(inputs=[start, end])
        successes += 1
    except BaseException:
        logger.exception(
            "Pipeline '%s' failed to process input: %s",
            pipeline.__repr_name__(),
            [start, end],
        )
        failures += 1
    logger.info(
        "Processing completed with %s successes, %s failures, and %s skipped.",
        successes,
        failures,
        skipped,
    )
    return successes, failures, skipped


if __name__ == "__main__":
    app()
