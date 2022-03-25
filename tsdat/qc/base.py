import numpy as np
import xarray as xr
from numpy.typing import NDArray
from abc import ABC, abstractmethod
from typing import Any, Dict
from tsdat.utils import ParametrizedClass


class QualityChecker(ParametrizedClass, ABC):
    @abstractmethod
    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:
        ...


class QualityHandler(ParametrizedClass, ABC):
    parameters: Dict[str, Any] = {}

    @abstractmethod
    def run(
        self, dataset: xr.Dataset, variable_name: str, results: NDArray[np.bool8]
    ) -> xr.Dataset:
        ...
