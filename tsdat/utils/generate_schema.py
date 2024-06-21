from pathlib import Path
from typing import Dict, Type, Union
import typer

from .standards_type import StandardsType


def generate_schema(
    dir: Path = typer.Option(
        Path(".vscode/schema/"),
        file_okay=False,
        dir_okay=True,
    ),
    standards: StandardsType = typer.Option(StandardsType.tsdat),
):
    from tsdat import (
        ACDDDatasetConfig,
        DatasetConfig,
        IOOSDatasetConfig,
        PipelineConfig,
        QualityConfig,
        RetrieverConfig,
        StorageConfig,
        YamlModel,
    )

    dir.mkdir(exist_ok=True)

    dataset_config_cls: Union[
        Type[ACDDDatasetConfig], Type[IOOSDatasetConfig], Type[DatasetConfig]
    ]
    if standards == StandardsType.acdd:
        dataset_config_cls = ACDDDatasetConfig
    elif standards == StandardsType.ioos:
        dataset_config_cls = IOOSDatasetConfig
    else:
        dataset_config_cls = DatasetConfig
    print(f"Using {standards} dataset standards")

    cls_mapping: Dict[str, Type[YamlModel]] = {
        "retriever": RetrieverConfig,
        "dataset": dataset_config_cls,
        "quality": QualityConfig,
        "storage": StorageConfig,
        "pipeline": PipelineConfig,
    }

    for cfg_type, cfg_cls in cls_mapping.items():
        path = dir / f"{cfg_type}-schema.json"
        cfg_cls.generate_schema(path)
        print(f"Wrote {cfg_type} schema files to {path}")
    print("Done!")
