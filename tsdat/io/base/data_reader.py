from abc import ABC, abstractmethod
from typing import (
    Dict,
    Pattern,
    Union,
)

import xarray as xr

from ...utils import (
    ParameterizedClass,
)


class DataReader(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for reading data from an input source.

    Args:
        regex (Pattern[str]): The regex pattern associated with the DataReader. If
        calling the DataReader from a tsdat pipeline, this pattern will be checked
        against each possible input key before the read() method is called.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def read(
        self,
        input_key: str,
    ) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        """-----------------------------------------------------------------------------
        Reads data given an input key.

        Uses the input key to open a resource and load data as a xr.Dataset object or as
        a mapping of strings to xr.Dataset objects.

        In most cases DataReaders will only need to return a single xr.Dataset, but
        occasionally some types of inputs necessitate that the data loaded from the
        input_key be returned as a mapping. For example, if the input_key is a path to a
        zip file containing multiple disparate datasets, then returning a mapping is
        appropriate.

        Args:
            input_key (str): An input key matching the DataReader's regex pattern that
                should be used to load data.

        Returns:
            Union[xr.Dataset, Dict[str, xr.Dataset]]: The raw data extracted from the
                provided input key.

        -----------------------------------------------------------------------------"""
        ...
