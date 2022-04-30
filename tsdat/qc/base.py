from typing import Callable, Dict, List
import numpy as np
from pydantic import BaseModel, Extra
import xarray as xr
from numpy.typing import NDArray
from abc import ABC, abstractmethod
from ..utils import ParameterizedClass

__all__ = ["QualityChecker", "QualityHandler", "QualityManager", "QualityManagement"]


class QualityChecker(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for code that checks the dataset / data variable quality.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:
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
            NDArray[np.bool8]: The results of the quality check, where True values
            indicate a quality problem.

        -----------------------------------------------------------------------------"""
        ...


class QualityHandler(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for code that handles the dataset / data variable quality.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Takes some action on data that has had quality issues identified.

        Handles the quality of a variable in the dataset and returns the dataset after
        any corrections have been applied.

        Args:
            dataset (xr.Dataset): The dataset containing the variable to handle.
            variable_name (str): The name of the variable whose quality should be
                handled.
            failures (NDArray[np.bool8]): The results of the QualityChecker for the
                provided variable, where True values indicate a quality problem.

        Returns:
            xr.Dataset: The dataset after the QualityHandler has been run.

        -----------------------------------------------------------------------------"""
        ...


class QualityManager(BaseModel, extra=Extra.forbid):
    """---------------------------------------------------------------------------------
    Groups a QualityChecker and one or more QualityHandlers together.

    Args:
        name (str): The name of the quality manager.
        checker (QualityChecker): The quality check that should be run.
        handlers (QualityHandler): One or more QualityHandlers that should be run given
            the results of the checker.
        apply_to (List[str]): A list of variables that the check should run for. Accepts
            keywords of 'COORDS' or 'DATA_VARS', or any number of specific variables that
            should be run.
        exclude (List[str]): A list of variables that the check should exclude. Accepts
            the same keywords as apply_to.

    ---------------------------------------------------------------------------------"""

    name: str
    checker: QualityChecker
    handlers: List[QualityHandler]
    apply_to: List[str]
    exclude: List[str] = []

    def run(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Runs the quality manager on the dataset.

        Args:
            dataset (xr.Dataset): The dataset to apply quality checks / controls to.

        Returns:
            xr.Dataset: The dataset after the quality check and controls have been
            applied.

        -----------------------------------------------------------------------------"""
        variables = self._get_variables_to_run(dataset)
        for variable_name in variables:
            issues = self.checker.run(dataset, variable_name)
            for handler in self.handlers:
                dataset = handler.run(dataset, variable_name, issues)
        return dataset

    def _get_variables_to_run(self, dataset: xr.Dataset) -> List[str]:
        keyword_map: Dict[str, Callable[[xr.Dataset], List[str]]] = {
            "COORDS": self._get_dataset_coords,
            "DATA_VARS": self._get_dataset_data_vars,
        }

        variables: List[str] = []
        for key in self.apply_to:
            if key in keyword_map:
                variables += keyword_map[key](dataset)
            else:
                variables.append(key)

        return [v for v in variables if v not in self.exclude]

    def _get_dataset_coords(self, dataset: xr.Dataset) -> List[str]:
        return list(dataset.coords)

    def _get_dataset_data_vars(self, dataset: xr.Dataset) -> List[str]:
        return [str(v) for v in dataset.data_vars if not str(v).startswith("qc_")]


class QualityManagement(BaseModel, extra=Extra.forbid):
    """---------------------------------------------------------------------------------
    Main class for orchestrating the dispatch of QualityCheckers and QualityHandlers.

    Args:
        managers (List[QualityManager]): The list of QualityManagers that should be run.

    ---------------------------------------------------------------------------------"""

    managers: List[QualityManager]

    def manage(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Runs the registered QualityManagers on the dataset.

        Args:
            dataset (xr.Dataset): The dataset to apply quality checks and controls to.

        Returns:
            xr.Dataset: The quality-checked dataset.

        -----------------------------------------------------------------------------"""
        for manager in self.managers:
            dataset = manager.run(dataset)
        return dataset
