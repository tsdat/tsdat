from typing import Callable, Dict, Hashable, List

import xarray as xr
from pydantic import BaseModel, Extra

from .quality_checker import QualityChecker
from .quality_handler import QualityHandler


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
            if issues is None:
                continue
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

    @staticmethod
    def _get_dataset_coords(dataset: xr.Dataset) -> List[Hashable]:
        return list(dataset.coords)

    @staticmethod
    def _get_dataset_data_vars(dataset: xr.Dataset) -> List[str]:
        return [str(v) for v in dataset.data_vars if not str(v).startswith("qc_")]
