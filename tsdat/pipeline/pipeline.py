from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel, BaseSettings, Extra
from tsdat.config.dataset import DatasetConfig
from tsdat.config.quality import QualityConfig
from tsdat.io.storage.storage import BaseStorage


class PipelineSettings(BaseSettings, extra=Extra.allow):
    retain_input_files: bool = False


class BasePipeline(BaseModel, ABC, extra=Extra.forbid):

    parameters: Any
    associations: List[Pattern] = []  # type: ignore # HACK: Pattern[str] when possible
    dataset: DatasetConfig
    quality: QualityConfig  # IDEA: Parametrize the config class; custom qc dispatch
    storage: BaseStorage
    settings: PipelineSettings = PipelineSettings()

    @abstractmethod
    def run(self, inputs: Any) -> Any:
        ...
