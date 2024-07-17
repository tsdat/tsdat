from pathlib import Path
from typing import Dict, Literal, Optional, Type, Union

import typer
from pydantic import BaseModel, Extra, Field

from ..config.dataset.acdd_dataset_config import ACDDDatasetConfig
from ..config.dataset.dataset_config import DatasetConfig
from ..config.dataset.ioos_dataset_config import IOOSDatasetConfig
from ..config.pipeline.pipeline_config import PipelineConfig
from ..config.quality.quality_config import QualityConfig
from ..config.retriever.retriever_config import RetrieverConfig
from ..config.storage.storage_config import StorageConfig
from ..config.utils.yaml_model import YamlModel
from ..utils.standards_type import StandardsType


class VapRetrieverConfig(RetrieverConfig):
    class Parameters(BaseModel, extra=Extra.forbid):
        class FetchParameters(BaseModel, extra=Extra.forbid):
            time_padding: str = Field(
                regex=r"^[\+|\-]?[0-9]+[h|m|s|ms]$",
                description=(
                    "The time_padding parameter in the fetch_parameters section"
                    " specifies how far in time to look for data before the 'begin'"
                    " timestamp (e.g., -24h), after the 'end' timestamp (e.g., +24h),"
                    " or both (e.g., 24h).  Units of hours ('h'), minutes ('m'),"
                    " seconds ('s', default), and milliseconds ('ms') are allowed."
                ),
            )

        class TransformationParameters(BaseModel, extra=Extra.forbid):
            alignment: dict[str, Literal["LEFT", "RIGHT", "CENTER"]] = Field(
                description=(
                    "Defines the location of the window in respect to each output"
                    " timestamp (LEFT, RIGHT, or CENTER)"
                )
            )

            dim_range: dict[str, str] = Field(
                ...,
                alias="range",
                regex=r"^[0-9]+[a-zA-Z]+$",
                description=(
                    "Defines how far (in seconds) from the first/last timestamp to "
                    "search for the previous/next measurement."
                ),
            )
            width: dict[str, str] = Field(
                ...,
                regex=r"^[0-9]+[a-zA-Z]+$",
                description=(
                    'Defines the size of the averaging window in seconds ("600s" = 10 '
                    "min)."
                ),
            )

        fetch_parameters: Optional[FetchParameters] = None
        transformation_parameters: Optional[TransformationParameters] = Field(
            default=None,
            description=(
                "Transformation parameters. See "
                "https://tsdat.readthedocs.io/en/stable/tutorials/vap_pipelines/#configuration-files-vap_gps"
                " for more information."
            ),
        )

    parameters: Optional[Parameters] = None  # type: ignore


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
