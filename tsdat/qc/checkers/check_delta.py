from abc import ABC
import numpy as np
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Any, Union
from numpy.typing import NDArray

from .threshold_checker import ThresholdChecker


class CheckDelta(ThresholdChecker, ABC):
    """---------------------------------------------------------------------------------
    Checks for deltas between consecutive values larger than a specified threshold.

    Checks the difference between consecutive values and reports a failure if the
    difference is less than the threshold specified by the value in the attribute
    provided to this check.

    Args:
        attribute_name (str): The name of the attribute containing the threshold to use.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        dim: str = "time"
        """The dimension on which to perform the diff."""

    parameters: Parameters = Parameters()

    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
    ) -> Union[NDArray[np.bool_], None]:
        var_data = dataset[variable_name]
        failures: NDArray[np.bool_] = np.zeros_like(var_data, dtype=np.bool_)  # type: ignore

        threshold = self._get_threshold(dataset, variable_name, True)
        if threshold is None:
            return None

        data: NDArray[Any] = var_data.data
        axis = var_data.get_axis_num(self.parameters.dim)

        prepend = np.expand_dims(np.take(data, 0, axis=axis), axis=axis)  # type: ignore
        diff: NDArray[Any] = np.absolute(np.diff(data, axis=axis, prepend=prepend))  # type: ignore
        failures = diff > threshold if self.allow_equal else diff >= threshold

        return failures
