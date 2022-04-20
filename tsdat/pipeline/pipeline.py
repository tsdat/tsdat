from abc import ABC, abstractmethod
from typing import Any, List, Pattern
from pydantic import Field
from tsdat.config.dataset import DatasetConfig
from tsdat.io.base import Retriever, Storage
from tsdat.qc.qc import QualityManagement
from tsdat.utils import ParametrizedClass


class Pipeline(ParametrizedClass, ABC):
    settings: Any = None

    parameters: Any = {}

    triggers: List[Pattern] = []  # type: ignore
    """Regex patterns matching input keys to determine when the pipeline should run."""

    retriever: Retriever
    """Retrieves data from input keys."""

    dataset_config: DatasetConfig = Field(alias="dataset")
    """Describes the structure and metadata of the output dataset."""

    quality: QualityManagement
    """Manages the dataset quality through checks and corrections."""

    storage: Storage
    """Stores the dataset so it can be retrieved later."""

    @abstractmethod
    def run(self, inputs: Any, **kwargs: Any) -> Any:
        """-----------------------------------------------------------------------------
        Runs the data pipeline on the provided inputs.

        Args:
            inputs (List[str]): A list of input keys that the pipeline's Retriever class
            can use to load data into the pipeline.

        Returns:
            xr.Dataset: The processed dataset.

        -----------------------------------------------------------------------------"""
        ...
