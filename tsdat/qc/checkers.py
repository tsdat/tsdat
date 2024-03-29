import logging
import numpy as np
import xarray as xr
from pydantic import BaseModel, Extra, validator
from typing import Any, Dict, List, Literal, Optional, Union
from numpy.typing import NDArray
from .base import QualityChecker


__all__ = [
    "CheckMissing",
    "CheckMonotonic",
    "CheckValidMin",
    "CheckValidMax",
    "CheckFailMin",
    "CheckFailMax",
    "CheckWarnMin",
    "CheckWarnMax",
    "CheckValidRangeMin",
    "CheckValidRangeMax",
    "CheckFailRangeMin",
    "CheckFailRangeMax",
    "CheckWarnRangeMin",
    "CheckWarnRangeMax",
    "CheckValidDelta",
    "CheckFailDelta",
    "CheckWarnDelta",
]

logger = logging.getLogger(__name__)


def is_datetime_like(data: NDArray[Any]) -> bool:
    """Checks if the array has a datetime-like dtype (datetime, timedelta, date)"""
    return np.issubdtype(data.dtype, (np.datetime64, np.timedelta64))


class CheckMissing(QualityChecker):
    """---------------------------------------------------------------------------------
    Checks if any data are missing. A variable's data are considered missing if they are
    set to the variable's _FillValue (if it has a _FillValue) or NaN (NaT for datetime-
    like variables).

    ---------------------------------------------------------------------------------"""

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool_]:
        results: NDArray[np.bool_] = dataset[variable_name].isnull().data

        if "_FillValue" in dataset[variable_name].attrs:
            fill_value = dataset[variable_name].attrs["_FillValue"]
            results |= dataset[variable_name].data == fill_value

        elif np.issubdtype(dataset[variable_name].data.dtype, str):  # type: ignore
            fill_value = ""
            results |= dataset[variable_name].data == fill_value

        return results


class CheckMonotonic(QualityChecker):
    """---------------------------------------------------------------------------------
    Checks if any values are not ordered strictly monotonically (i.e. values must all be
    increasing or all decreasing). The check marks values as failed if they break from
    a monotonic order.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        require_decreasing: bool = False
        require_increasing: bool = False
        dim: Optional[str] = None

        @validator("require_increasing")
        @classmethod
        def check_monotonic_not_increasing_and_decreasing(
            cls, inc: bool, values: Dict[str, Any]
        ) -> bool:
            if inc and values["require_decreasing"]:
                raise ValueError(
                    "CheckMonotonic -> Parameters: cannot set both 'require_increasing'"
                    " and 'require_decreasing'. Please set one or both to False."
                )
            return inc

    parameters: Parameters = Parameters()

    def run(
        self, dataset: xr.Dataset, variable_name: str
    ) -> Union[NDArray[np.bool_], None]:
        variable = dataset[variable_name]
        failures = np.full(variable.shape, False)

        if self.parameters.dim is None and len(variable.shape) == 2:
            logger.warning(
                "Variable '%s' has shape '%s'. 2D variables must provide a 'dim'"
                " parameter for the name of the dimension to check the variable for"
                " monotonicity.",
                variable_name,
                variable.shape,
            )
            return None

        if variable.values.dtype.kind in {"U", "S"}:  # type: ignore
            logger.warning(
                "Variable '%s' has dtype '%s', which is currently not supported for"
                " monotonicity checks.",
                variable_name,
                variable.values.dtype,  # type: ignore
            )
            return None

        axis = self.get_axis(variable)
        zero = np.timedelta64(0) if is_datetime_like(variable.data) else 0
        direction: Literal["increasing", "decreasing", ""] = ""

        if self.parameters.require_decreasing:
            direction = "decreasing"
        elif self.parameters.require_increasing:
            direction = "increasing"
        else:
            diff = np.diff(variable.data, axis=axis)  # type: ignore
            direction = (
                "increasing"
                if np.sum(diff > zero) >= np.sum(diff < zero)
                else "decreasing"
            )

        # Find all the values where things break, not just those flagged by diff
        # if any(failures) and not all(failures):
        if len(variable.shape) == 1:
            prev = variable.values[0]
            for i, value in enumerate(variable.values[1:]):
                success = value < prev if direction == "decreasing" else value > prev
                if success:
                    prev = value  # only update prev on success
                else:
                    failures[i + 1] = True
        else:
            # 2D diff isn't as clever with failing indexes; just report all individual
            # points that fail
            diff = np.gradient(variable.data)[axis]
            failures = diff <= zero if direction == "increasing" else diff >= zero

        return failures

    def get_axis(self, variable: xr.DataArray) -> int:
        if not (dim := self.parameters.dim):
            dim = variable.dims[0]  # type: ignore
        return variable.get_axis_num(dim)  # type: ignore


class _ThresholdChecker(QualityChecker):
    """---------------------------------------------------------------------------------
    Base class for checks that use a variable attribute to specify a threshold.

    Args:
        attribute_name (str): The name of the attribute containing the maximum
            threshold. If the attribute ends in '_range' then it is assumed to be a
            list, and the first value from the list will be used as the minimum
            threshold.
        allow_equal (bool): True if values equal to the threshold should pass the check,
            False otherwise.

    ---------------------------------------------------------------------------------"""

    allow_equal: bool = True
    """True if values equal to the threshold should pass, False otherwise."""

    attribute_name: str
    """The attribute on the data variable that should be used to get the threshold."""

    def _get_threshold(
        self, dataset: xr.Dataset, variable_name: str, min_: bool
    ) -> Union[float, None]:
        threshold = dataset[variable_name].attrs.get(self.attribute_name, None)
        if threshold is not None:
            if isinstance(threshold, list):
                index = 0 if min_ else -1
                threshold = threshold[index]
        return threshold


class _CheckMin(_ThresholdChecker):
    """---------------------------------------------------------------------------------
    Checks for values less than a specified threshold.

    The value of the threshold is specified by an attribute on each data variable, and
    the attribute to search for is specified as a property of this base class.

    If the specified attribute does not exist on the variable being checked then no
    failures will be reported.

    Args:
        attribute_name (str): The name of the attribute containing the minimum
            threshold. If the attribute ends in '_range' then it is assumed to be a
            list, and the first value from the list will be used as the minimum
            threshold.
        allow_equal (bool): True if values equal to the threshold should pass the check,
            False otherwise.

    ---------------------------------------------------------------------------------"""

    def run(
        self, dataset: xr.Dataset, variable_name: str
    ) -> Union[NDArray[np.bool_], None]:
        var_data = dataset[variable_name]
        failures: NDArray[np.bool_] = np.zeros_like(var_data, dtype=np.bool_)  # type: ignore

        min_value = self._get_threshold(dataset, variable_name, min_=True)
        if min_value is None:
            return None

        if self.allow_equal:
            failures = np.less(var_data.data, min_value)
        else:
            failures = np.less_equal(var_data.data, min_value)

        return failures


class _CheckMax(_ThresholdChecker):
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
        self, dataset: xr.Dataset, variable_name: str
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


class CheckValidMin(_CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'valid_min'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_min"


class CheckValidMax(_CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'valid_max'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_max"


class CheckFailMin(_CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'fail_min'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_min"


class CheckFailMax(_CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'fail_max'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_max"


class CheckWarnMin(_CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'warn_min'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_min"


class CheckWarnMax(_CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'warn_max'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_max"


class CheckValidRangeMin(_CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'valid_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_range"


class CheckValidRangeMax(_CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'valid_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_range"


class CheckFailRangeMin(_CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'fail_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_range"


class CheckFailRangeMax(_CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'fail_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_range"


class CheckWarnRangeMin(_CheckMin):
    """------------------------------------------------------------------------------------
    Checks for values less than 'warn_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_range"


class CheckWarnRangeMax(_CheckMax):
    """------------------------------------------------------------------------------------
    Checks for values greater than 'warn_range'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_range"


class _CheckDelta(_ThresholdChecker):
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
        self, dataset: xr.Dataset, variable_name: str
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


class CheckValidDelta(_CheckDelta):
    """------------------------------------------------------------------------------------
    Checks for deltas between consecutive values larger than 'valid_delta'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "valid_delta"


class CheckFailDelta(_CheckDelta):
    """------------------------------------------------------------------------------------
    Checks for deltas between consecutive values larger than 'fail_delta'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "fail_delta"


class CheckWarnDelta(_CheckDelta):
    """------------------------------------------------------------------------------------
    Checks for deltas between consecutive values larger than 'warn_delta'.

    ------------------------------------------------------------------------------------
    """

    attribute_name: str = "warn_delta"


# check_outlier(std_dev)
# check_time_gap --> parameters: min_time_gap (str), max_time_gap (str)
