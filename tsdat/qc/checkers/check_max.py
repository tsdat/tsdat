from abc import ABC
import numpy as np
import xarray as xr
from typing import Union
from numpy.typing import NDArray

from .threshold_checker import ThresholdChecker


class CheckMax(ThresholdChecker, ABC):
    """---------------------------------------------------------------------------------
    Checks for values larger than a specified threshold.

    The value of the threshold is specified by an attribute on each data variable, and
    the attribute to search for is specified as a property of this base class.

    If the specified attribute does not exist on the variable being checked then no
    failures will be reported.

    Args:
        attribute_name (str): The name of the attribute containing the maximum
            threshold. If the attribute ends in '_range' then it is assumed to be a
            list, and the first value from the list will be used as the minimum
            threshold.
        allow_equal (bool): True if values equal to the threshold should pass the check,
            False otherwise.

    ---------------------------------------------------------------------------------"""

    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
    ) -> Union[NDArray[np.bool_], None]:
        var_data = dataset[variable_name]
        failures: NDArray[np.bool_] = np.zeros_like(var_data, dtype=np.bool_)  # type: ignore

        max_value = self._get_threshold(dataset, variable_name, min_=False)
        if max_value is None:
            return None

        if self.allow_equal:
            failures = np.greater(var_data.data, max_value)
        else:
            failures = np.greater_equal(var_data.data, max_value)

        return failures
