from abc import ABC, abstractmethod
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Pattern,
)

import xarray as xr

from ...config.dataset import DatasetConfig
from ...utils import (
    ParameterizedClass,
)
from .retrieved_variable import RetrievedVariable


class Retriever(ParameterizedClass, ABC):
    """Base class for retrieving data used as input to tsdat pipelines."""

    readers: Optional[Dict[Pattern, Any]]  # type: ignore
    """The mapping of readers that should be used to retrieve data given input_keys and
    optional keyword arguments provided by subclasses of Retriever."""

    coords: Dict[str, Dict[Pattern, RetrievedVariable]]  # type: ignore
    """A dictionary mapping output coordinate names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each
    retrieved coordinate variable."""

    data_vars: Dict[str, Dict[Pattern, RetrievedVariable]]  # type: ignore
    """A dictionary mapping output data variable names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each
    retrieved data variable."""

    @abstractmethod
    def retrieve(
        self, input_keys: List[str], dataset_config: DatasetConfig, **kwargs: Any
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Prepares the raw dataset mapping for use in downstream pipeline processes.

        This is done by consolidating the data into a single xr.Dataset object. The
        retrieved dataset may contain additional coords and data_vars that are not
        defined in the output dataset. Input data converters are applied as part of the
        preparation process.

        Args:
            input_keys (List[str]): The input keys the registered DataReaders should
                read from.
            dataset_config (DatasetConfig): The specification of the output dataset.

        Returns:
            xr.Dataset: The retrieved dataset.

        -----------------------------------------------------------------------------"""
        ...

    def match_coord(self, var_name: str, input_key: str) -> RetrievedVariable | None:
        mapping = self.coords[var_name]
        for pattern, ret_var in mapping.items():
            if pattern.match(input_key):
                return ret_var

    def match_data_var(self, var_name: str, input_key: str) -> RetrievedVariable | None:
        mapping = self.data_vars[var_name]
        for pattern, ret_var in mapping.items():
            if pattern.match(input_key):
                return ret_var
