from abc import ABC, abstractmethod
from typing import (
    Any,
    Optional,
)

import xarray as xr

from .retrieved_dataset import RetrievedDataset
from ...config.dataset import DatasetConfig
from ...utils import (
    ParameterizedClass,
)


class DataConverter(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for running data conversions on retrieved raw data.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: DatasetConfig,
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        """-----------------------------------------------------------------------------
        Runs the data converter on the retrieved data.

        Args:
            data (xr.DataArray): The retrieved DataArray to convert.
            retrieved_dataset (RetrievedDataset): The retrieved dataset containing data
                to convert.
            dataset_config (DatasetConfig): The output dataset configuration.
            variable_name (str): The name of the variable to convert.

        Returns:
            Optional[xr.DataArray]: The converted DataArray for the specified variable,
                or None if the conversion was done in-place.

        -----------------------------------------------------------------------------"""
        ...
