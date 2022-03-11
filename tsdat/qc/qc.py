import numpy as np
import xarray as xr
from abc import ABC, abstractmethod
from typing import Any, Dict, List
from pydantic import BaseModel, Extra

from tsdat.config.dataset import DatasetConfig


class BaseQualityChecker(BaseModel, ABC, extra=Extra.forbid):
    parameters: Dict[str, Any] = {}

    @abstractmethod
    def run(self, dataset: xr.Dataset, variable: str) -> np.ndarray[bool, Any]:
        ...


class BaseQualityHandler(BaseModel, ABC, extra=Extra.forbid):
    parameters: Dict[str, Any] = {}

    @abstractmethod
    def run(
        self, dataset: xr.Dataset, variable: str, results: np.ndarray[bool, Any]
    ) -> xr.Dataset:
        ...


class QualityManager(BaseModel, extra=Extra.forbid):
    name: str
    checker: BaseQualityChecker
    handlers: List[BaseQualityHandler]
    apply_to: List[str]
    exclude: List[str]

    def run(self, dataset: xr.Dataset, dataset_config: DatasetConfig) -> xr.Dataset:
        output = dataset
        variables = self.get_variables_to_run(
            apply_to=self.apply_to, exclude=self.exclude, dataset_config=dataset_config
        )
        for variable in variables:
            quality_issues = self.checker.run(dataset=output, variable=variable)
            for handler in self.handlers:
                output = handler.run(
                    dataset=output, variable=variable, results=quality_issues
                )
        return output

    def get_variables_to_run(
        self, apply_to: List[str], exclude: List[str], dataset_config: DatasetConfig
    ) -> List[str]:
        # IDEA: This function can be turned into a validator

        keyword_map: Dict[str, List[str]] = {
            "COORDS": [coord.name for coord in dataset_config.coords],
            "DATA_VARS": [var.name for var in dataset_config.data_vars],
        }

        for keyword, variables in keyword_map.items():
            if keyword in apply_to:
                apply_to.remove(keyword)
                apply_to += variables

        apply_to = [name for name in apply_to if name not in exclude]

        # TODO: Remove this when validation of duplicates is done at the config level
        apply_to = list(dict.fromkeys(apply_to))

        return apply_to


class QualityRegistry(BaseModel, extra=Extra.forbid):
    # IDEA: Adapt to allow users to plug-in custom quality registries. This would allow
    # users to customize how quality dispatch is done (e.g., to support parallel
    # dispatch, or other custom rulesets).
    managers: List[QualityManager]

    def manage(self, dataset: xr.Dataset, dataset_config: DatasetConfig) -> xr.Dataset:
        output = dataset
        for manager in self.managers:
            output = manager.run(output, dataset_config)
        return output
