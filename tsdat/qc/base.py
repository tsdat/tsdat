import numpy as np
import xarray as xr
from numpy.typing import NDArray
from abc import ABC, abstractmethod
from typing import Any, Dict
from tsdat.utils import ParametrizedClass


class QualityChecker(ParametrizedClass, ABC):
    @abstractmethod
    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:
        """-----------------------------------------------------------------------------
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
            NDArray[np.bool8]: The results of the quality check, where True values
            indicate a quality problem.

        --------------------------------------------------------------------------------"""
        ...


class QualityHandler(ParametrizedClass, ABC):
    @abstractmethod
    def run(
        self, dataset: xr.Dataset, variable_name: str, results: NDArray[np.bool8]
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Handles the quality of a variable in the dataset and returns the dataset after
        any corrections have been applied.

        Args:
            dataset (xr.Dataset): The dataset containing the variable to handle.
            variable_name (str): The name of the variable whose quality should be
            handled.
            results (NDArray[np.bool8]): The results of the QualityChecker for the
            provided variable, where True values indicate a quality problem.

        Returns:
            xr.Dataset: _description_

        -----------------------------------------------------------------------------"""
        ...
