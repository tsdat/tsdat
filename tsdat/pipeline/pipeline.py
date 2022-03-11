from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel, BaseSettings, Extra, Field
from tsdat.config.dataset import DatasetConfig
from tsdat.io.storage.storage import BaseStorage
from tsdat.qc.qc import QualityRegistry


class PipelineSettings(BaseSettings, extra=Extra.allow):
    retain_input_files: bool = False


class BasePipeline(BaseModel, ABC, extra=Extra.forbid):

    parameters: Any
    associations: List[Pattern] = []  # type: ignore # HACK: Pattern[str] when possible
    # TODO: Type hinting for converters, other objects on instantiated dataset object
    dataset_config: DatasetConfig = Field(alias="dataset")
    quality: QualityRegistry  # TODO: Make this optional (everywhere)
    storage: BaseStorage
    settings: PipelineSettings = PipelineSettings()

    @abstractmethod
    def run(self, inputs: Any) -> Any:
        ...
