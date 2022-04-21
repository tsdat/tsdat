import xarray as xr
from typing import Callable, Dict, List
from pydantic import BaseModel, Extra
from .base import QualityChecker, QualityHandler


class QualityManager(BaseModel, extra=Extra.forbid):
    name: str
    checker: QualityChecker
    handlers: List[QualityHandler]
    apply_to: List[str]
    exclude: List[str] = []

    def run(self, dataset: xr.Dataset) -> xr.Dataset:
        variables = self.get_variables_to_run(dataset)
        for variable_name in variables:
            issues = self.checker.run(dataset, variable_name)
            for handler in self.handlers:
                dataset = handler.run(dataset, variable_name, issues)
        return dataset

    def get_variables_to_run(self, dataset: xr.Dataset) -> List[str]:
        def _get_dataset_coords(dataset: xr.Dataset) -> List[str]:
            return list(dataset.coords)

        def _get_dataset_data_vars(dataset: xr.Dataset) -> List[str]:
            return [str(v) for v in dataset.data_vars if not str(v).startswith("qc_")]

        keyword_map: Dict[str, Callable[[xr.Dataset], List[str]]] = {
            "COORDS": _get_dataset_coords,
            "DATA_VARS": _get_dataset_data_vars,
        }

        variables: List[str] = []
        for key in self.apply_to:
            if key in keyword_map:
                variables += keyword_map[key](dataset)
            else:
                variables.append(key)

        return [v for v in variables if v not in self.exclude]


class QualityManagement(BaseModel, extra=Extra.forbid):
    managers: List[QualityManager]

    def manage(self, dataset: xr.Dataset) -> xr.Dataset:
        for manager in self.managers:
            dataset = manager.run(dataset)
        return dataset
