from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any
from pydantic import BaseModel, BaseSettings, Extra
from tsdat.config.dataset import DatasetConfig
from tsdat.config.pipeline import Config
from tsdat.config.quality import QualityConfig
from tsdat.io.storage.storage import AbstractStorage


# How to instantiate a Pipeline class:
# - pass paths to dataset, quality, and storage config files as kwargs, + settings
# - instantiate a PipelineConfig object, then Pipeline(config)
# - PipelineDefinition.from_yaml(path_to_pipeline_config).instantiate()


class PipelineSettings(BaseSettings, extra=Extra.allow):
    retain_input_files: bool = False


class AbstractPipeline(BaseModel, ABC, extra=Extra.allow):

    parameters: Any
    associations: List[Pattern] = []  # type: ignore # HACK: Pattern[str] if possible
    dataset: DatasetConfig
    quality: QualityConfig  # TODO: Parametrize the config class; custom qc dispatch
    storage: AbstractStorage
    settings: PipelineSettings = PipelineSettings()

    @abstractmethod
    def run(self, inputs: Any) -> Any:
        ...

    # @classmethod
    # def from_yaml(
    #     cls,
    #     dataset_path: Path,
    #     storage_path: Path,
    #     quality_path: Path,
    #     validate: bool = True,
    #     **settings: Any
    # ):
    #     config = Config.from_yaml(
    #         dataset_path=dataset_path,
    #         quality_path=quality_path,
    #         storage_path=storage_path,
    #         validate=validate,
    #         **settings
    #     )
    #     storage = config.storage.instantiate()
    #     return cls(config=config, storage=storage)
