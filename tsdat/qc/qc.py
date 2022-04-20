from copy import copy
import xarray as xr
from typing import Dict, List
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
        variables: List[str] = copy(self.apply_to)

        keyword_map: Dict[str, List[str]] = {
            "COORDS": list(dataset.coords.keys()),
            "DATA_VARS": list(dataset.data_vars.keys()),
        }

        n_insertions = 0
        for i in range(len(self.apply_to)):
            if self.apply_to[i] in keyword_map:
                to_insert = keyword_map[self.apply_to[i]]
                variables[i + n_insertions : i + n_insertions] = to_insert
                n_insertions += len(to_insert)

        return [var for var in variables if var not in self.exclude]


class QualityManagement(BaseModel, extra=Extra.forbid):
    managers: List[QualityManager]

    def manage(self, dataset: xr.Dataset) -> xr.Dataset:
        for manager in self.managers:
            dataset = manager.run(dataset)
        return dataset
