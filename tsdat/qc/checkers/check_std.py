from abc import ABC
from typing import Union

import numpy as np
import xarray as xr
from numpy.typing import NDArray

from .threshold_checker import ThresholdChecker


class CheckStd(ThresholdChecker, ABC):
    """Checks data for elements greater than a specified number of standard deviations
    away from the mean.

    The value of the threshold is specified by an attribute on each data variable, and
    the attribute to search for is specified as a property of this base class.

    If the specified attribute does not exist on the variable being checked then no
    failures will be reported."""

    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
    ) -> Union[NDArray[np.bool_], None]:

        var_data = dataset[variable_name]
        if hasattr(var_data, "_FillValue"):
            var_data = var_data.where(
                dataset[variable_name] != dataset[variable_name]._FillValue
            )
        failures: NDArray[np.bool_] = np.zeros_like(var_data, dtype=np.bool_)  # type: ignore

        threshold = self._get_threshold(dataset, variable_name, True)
        if threshold is None:
            return None

        std_dev = var_data.std(dim="time", ddof=1)
        mean = var_data.mean(dim="time")
        failures = (var_data > mean + std_dev * threshold).values

        return failures