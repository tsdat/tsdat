from abc import ABC, abstractmethod
from typing import (
    Any,
)

import xarray as xr
from ...utils import ParameterizedClass


class DataWriter(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for writing data to storage area(s).

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def write(self, dataset: xr.Dataset, **kwargs: Any) -> None:
        """-----------------------------------------------------------------------------
        Writes the dataset to the storage area.

        This method is typically called by the tsdat storage API, which will be
        responsible for providing any additional parameters required by subclasses of
        the tsdat.io.base.DataWriter class.

        Args:
            dataset (xr.Dataset): The dataset to save.

        -----------------------------------------------------------------------------"""
        ...
