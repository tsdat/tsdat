from abc import ABC
from typing import Union

import numpy as np
import xarray as xr
from numpy.typing import NDArray

from .threshold_checker import ThresholdChecker


class CheckMin(ThresholdChecker, ABC):
    """Checks for values less than a specified threshold.

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

        min_value = self._get_threshold(dataset, variable_name, min_=True)
        if min_value is None:
            return None

        if self.allow_equal:
            failures = np.less(var_data.data, min_value)
        else:
            failures = np.less_equal(var_data.data, min_value)

        return failures
