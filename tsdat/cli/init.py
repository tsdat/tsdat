"""Initialize a standalone pipeline repository from packaged template resources."""

from importlib import resources
from pathlib import Path
from typing import Annotated

import typer

try:
    from importlib.resources.abc import Traversable
except ModuleNotFoundError:  # Python 3.10
    from importlib.abc import Traversable


def _copy_tree(source: Traversable, destination: Path) -> None:
    for entry in source.iterdir():
        target = destination / entry.name
        if entry.is_dir():
            target.mkdir()
            _copy_tree(entry, target)
        else:
            target.write_bytes(entry.read_bytes())


def init(
    destination: Annotated[
        Path | None, typer.Argument(help="Path for the new pipeline repository.")
    ] = None,
) -> None:
    """Create a standalone pipeline repository with a working example."""
    if destination is None:
        destination = Path(
            typer.prompt("Where should the new pipeline repository be created?")
        )
    if destination.exists() or destination.is_symlink():
        raise typer.BadParameter(f"Destination already exists: {destination}")

    template = resources.files("tsdat").joinpath("templates", "pipeline_project")
    if not template.is_dir():
        raise RuntimeError("Packaged pipeline project template is missing.")

    destination = destination.absolute()
    destination.mkdir(parents=True)
    try:
        _copy_tree(template, destination)
    except Exception:
        # Never remove a pre-existing directory; this command only creates new ones.
        import shutil

        shutil.rmtree(destination)
        raise

    typer.echo(f"Created pipeline repository at {destination}")
    typer.echo(f"Next: cd {destination} && pip install -r requirements-dev.txt")
    typer.echo("Then: tsdat create-pipeline ingest (or vap)")
