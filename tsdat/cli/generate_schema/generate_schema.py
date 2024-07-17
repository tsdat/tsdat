from pathlib import Path
from typing import Dict, Type, Union

import typer

from ...config.dataset.acdd_dataset_config import ACDDDatasetConfig
from ...config.dataset.dataset_config import DatasetConfig
from ...config.dataset.ioos_dataset_config import IOOSDatasetConfig
from ...config.pipeline.pipeline_config import PipelineConfig
from ...config.quality.quality_config import QualityConfig
from ...config.retriever.retriever_config import RetrieverConfig
from ...config.storage.storage_config import StorageConfig
from ...config.utils.yaml_model import YamlModel
from ...utils.standards_type import StandardsType
from .vap_retriever_config import VapRetrieverConfig


def generate_schema(
    dir: Path = typer.Option(
        Path(".vscode/schema/"),
        file_okay=False,
        dir_okay=True,
    ),
    standards: StandardsType = typer.Option(StandardsType.tsdat),
):
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
        "vap-retriever": VapRetrieverConfig,
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
