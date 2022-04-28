#!/usr/bin/env python3
import typer
from typing import Any, Dict, List
from pathlib import Path
from enum import Enum
from tsdat import (
    DatasetConfig,
    QualityConfig,
    RetrieverConfig,
    StorageConfig,
    PipelineConfig,
    YamlModel,
)

__all__ = ["generate_schema"]

app = typer.Typer(add_completion=False)


class SchemaType(str, Enum):
    retriever = "retriever"
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
    schema_type: SchemaType = typer.Option(SchemaType.all),
):
    dir.mkdir(exist_ok=True)
    cls_mapping: Dict[str, Any] = {
        "retriever": RetrieverConfig,
        "dataset": DatasetConfig,
        "quality": QualityConfig,
        "storage": StorageConfig,
        "pipeline": PipelineConfig,
    }

    keys: List[str] = []
    if schema_type == "all":
        keys = list(cls_mapping.keys())
    else:
        keys = [schema_type]

    for key in keys:
        path = dir / f"{key}-schema.json"
        cls: YamlModel = cls_mapping[key]
        cls.generate_schema(path)
        print(f"Wrote {key} schema file to {path}")
    print("Done!")


@app.callback()
def callback():
    pass


if __name__ == "__main__":
    app()
