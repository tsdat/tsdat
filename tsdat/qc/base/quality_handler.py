from abc import ABC, abstractmethod

import numpy as np
import xarray as xr
from numpy.typing import NDArray

from ...utils import ParameterizedClass


class QualityHandler(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for code that handles the dataset / data variable quality.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
        failures: NDArray[np.bool_],
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Takes some action on data that has had quality issues identified.

        Handles the quality of a variable in the dataset and returns the dataset after
        any corrections have been applied.

        Args:
            dataset (xr.Dataset): The dataset containing the variable to handle.
            variable_name (str): The name of the variable whose quality should be
                handled.
            failures (NDArray[np.bool_]): The results of the QualityChecker for the
                provided variable, where True values indicate a quality problem.

        Returns:
            xr.Dataset: The dataset after the QualityHandler has been run.

        -----------------------------------------------------------------------------"""
        ...
