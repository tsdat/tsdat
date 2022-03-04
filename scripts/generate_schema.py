import typer
from typing import Any, Dict, List
from pathlib import Path
from enum import Enum
from tsdat.config.dataset import DatasetDefinition
from tsdat.config.quality import QualityDefinition
from tsdat.config.storage import StorageConfig
from tsdat.config.pipeline import PipelineDefinition
from tsdat.config.utils import YamlModel


app = typer.Typer(add_completion=False)


class SchemaType(str, Enum):
    dataset = "dataset"
    quality = "quality"
    storage = "storage"
    pipeline = "pipeline"
    all = "all"


@app.command()
def generate_schema(
    dir: Path = typer.Option(
        Path(".vscode/schema/"),
        file_okay=False,
        dir_okay=True,
    ),
    which: SchemaType = typer.Option(SchemaType.all),
):
    dir.mkdir(exist_ok=True)
    cls_mapping: Dict[str, Any] = {
        "dataset": DatasetDefinition,
        "quality": QualityDefinition,
        "storage": StorageConfig,
        "pipeline": PipelineDefinition,
    }
    keys: List[str] = []
    if which == "all":
        keys = ["dataset", "quality", "storage", "pipeline"]
    else:
        keys = [which]

    for key in keys:
        path = dir / f"{key}-schema.json"
        cls: YamlModel = cls_mapping[key]
        cls.generate_schema(path)
        print(f"Wrote schema file to {path}")
    print("Done!")


if __name__ == "__main__":
    app()
