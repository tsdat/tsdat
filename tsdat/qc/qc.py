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
        # TODO:
        # 1. Resolve the list of variables to run
        # 2. Call self.QualityChecker.run() with appropriate args
        # 3. Iteratively call self.QualityHandler[X].run() with appropriate args
        # 4. Return output dataset
        ...


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
