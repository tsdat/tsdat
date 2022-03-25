from abc import ABC, abstractmethod
from typing import Any, List, Pattern
from pydantic import Field
from tsdat.config.dataset import DatasetConfig
from tsdat.io.base import Retriever, Storage
from tsdat.qc.qc import QualityManagement
from tsdat.utils import ParametrizedClass  # TODO: Quality Management


class Pipeline(ParametrizedClass, ABC):
    settings: Any
    parameters: Any
    associations: List[Pattern[str]] = []

    retriever: Retriever
    dataset_config: DatasetConfig = Field(alias="dataset")
    quality: QualityManagement
    storage: Storage

    @abstractmethod
    def run(self, inputs: Any, **kwargs: Any) -> Any:
        ...
