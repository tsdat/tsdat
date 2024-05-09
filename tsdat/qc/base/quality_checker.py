from abc import ABC, abstractmethod
from typing import Union

import numpy as np
import xarray as xr
from numpy.typing import NDArray

from ...utils import ParameterizedClass


class QualityChecker(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for code that checks the dataset / data variable quality.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
    ) -> Union[NDArray[np.bool_], None]:
        """-----------------------------------------------------------------------------
        Identifies and flags quality problems with the data.

        Checks the quality of a specific variable in the dataset and returns the results
        of the check as a boolean array where True values represent quality problems and
        False values represent data that passes the quality check.

        QualityCheckers should not modify dataset variables; changes to the dataset
        should be made by QualityHandler(s), which receive the results of a
        QualityChecker as input.

        Args:
            dataset (xr.Dataset): The dataset containing the variable to check.
            variable_name (str): The name of the variable to check.

        Returns:
            NDArray[np.bool_]: The results of the quality check, where True values
            indicate a quality problem.

        -----------------------------------------------------------------------------"""
        ...
