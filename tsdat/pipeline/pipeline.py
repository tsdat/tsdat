from abc import ABC, abstractmethod
from typing import Any, List
from pydantic import BaseModel, BaseSettings, Extra, Field, validator
from pydantic.fields import ModelField
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
    # retriever: BaseRetriever
    storage: BaseStorage
    settings: PipelineSettings = PipelineSettings()

    @abstractmethod
    def run(self, inputs: Any) -> Any:
        ...

    @validator("storage")
    @classmethod
    def validate_storage(
        cls, v: BaseStorage, field: ModelField, values: List[Any]
    ) -> BaseStorage:
        return v
