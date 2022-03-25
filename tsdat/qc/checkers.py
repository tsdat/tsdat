# TODO: Implement CheckMin/CheckMax & warn/fail/valid


import numpy as np
from pydantic import BaseModel, validator
import xarray as xr
from numpy.typing import NDArray
from typing import Any, Dict, Optional
from .base import QualityChecker


class CheckMissing(QualityChecker):
    """------------------------------------------------------------------------------------
    Checks if any values are missing (i.e. NaN, or NaT for datetime variables).
    ------------------------------------------------------------------------------------"""

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:
        return dataset[variable_name].isnull().data


class CheckMonotonic(QualityChecker):
    """------------------------------------------------------------------------------------
    Checks if any values are not ordered strictly monotonically (i.e. values must all be
    increasing or all decreasing). The check marks all values as failed if any data values
    are not ordered monotonically.
    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel):
        require_decreasing: bool = False
        require_increasing: bool = False
        dim: Optional[str] = None

        @validator("require_increasing")
        @classmethod
        def monotonic_increasing_xor_decreasing(
            cls, inc: bool, values: Dict[str, Any]
        ) -> bool:
            if inc and values["require_decreasing"]:
                raise ValueError(
                    "CheckMonotonic -> Parameters: cannot set both 'require_increasing'"
                    " and 'require_decreasing'. Please set one or both to False."
                )
            return inc

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:
        variable = dataset[variable_name]

        axis = self.get_axis(variable)
        diff: NDArray[Any] = np.diff(variable.data, axis=axis)  # type: ignore

        zero: Any = 0
        if np.issubdtype(variable.data.dtype, (np.datetime64, np.timedelta64)):  # type: ignore
            zero = np.timedelta64(0)

        increasing: bool = np.all(diff > zero)  # type: ignore
        decreasing: bool = np.all(diff < zero)  # type: ignore

        if self.parameters.require_increasing:
            is_monotonic = increasing
        elif self.parameters.require_decreasing:
            is_monotonic = decreasing
        else:
            is_monotonic = increasing | decreasing

        return np.full(variable_data.shape, not is_monotonic, dtype=np.bool8)  # type: ignore

    def get_axis(self, variable: xr.DataArray) -> int:
        if not (dim := self.parameters.dim):
            dim = variable.dims[0]
        return variable.get_axis_num(dim)  # type: ignore


# class CheckMonotonic(QualityChecker):
#     """Checks that all values for the specified variable are either
#     strictly increasing or strictly decreasing.
#     """

#     def run(self, variable_name: str) -> Optional[NDArray[Any]]:


# class CheckMin(QualityChecker):
#     """Check that no values for the specified variable are less than
#     a specified minimum threshold.  The threshold value is an attribute
#     set on the variable in question. The  attribute name is
#     specified in the quality checker definition in the pipeline config
#     file by setting a param called 'key: ATTRIBUTE_NAME'.

#     If the key parameter is not set or the variable does not possess
#     the specified attribute, this check will be skipped.
#     """

#     def __init__(
#         self, ds: xr.Dataset, definition: ManagerConfig, parameters: Dict[str, Any]
#     ):
#         super().__init__(ds, definition, parameters)
#         self.key: str = self.params["key"]

#     def run(self, variable_name: str) -> Optional[NDArray[Any]]:
#         results_array: Optional[NDArray[Any]] = None
#         min_value: Optional[Union[float, List[float]]] = self.ds[
#             variable_name
#         ].attrs.get(self.key, None)

#         if min_value is not None:
#             if isinstance(min_value, List):
#                 min_value = min_value[0]

#             results_array = np.less(self.ds[variable_name].data, min_value)

#         return results_array


# class CheckMax(QualityChecker):
#     """Check that no values for the specified variable are greater than
#     a specified maximum threshold.  The threshold value is an attribute
#     set on the variable in question. The  attribute name is
#     specified in the quality checker definition in the pipeline config
#     file by setting a param called 'key: ATTRIBUTE_NAME'.

#     If the key parameter is not set or the variable does not possess
#     the specified attribute, this check will be skipped.
#     """

#     def __init__(
#         self, ds: xr.Dataset, definition: ManagerConfig, parameters: Dict[str, Any]
#     ):
#         super().__init__(ds, definition, parameters)
#         self.key: str = self.params["key"]

#     def run(self, variable_name: str) -> Optional[NDArray[Any]]:
#         results_array: Optional[NDArray[Any]] = None
#         max_value: Optional[Union[float, List[float]]] = self.ds[
#             variable_name
#         ].attrs.get(self.key, None)

#         if max_value is not None:
#             if isinstance(max_value, List):
#                 max_value = max_value[1]

#             results_array = np.greater(self.ds[variable_name].data, max_value)

#         return results_array


# # TODO: Initialize the remaining classes automatically


# class CheckValidMin(CheckMin):
#     """Check that no values for the specified variable are less than
#     the minimum vaue set by the 'valid_range' attribute.  If the
#     variable in question does not posess the 'valid_range' attribute,
#     this check will be skipped.
#     """

#     def __init__(
#         self,
#         ds: xr.Dataset,
#         definition: ManagerConfig,
#         parameters: Dict[str, Any],
#     ):
#         super().__init__(ds, definition, parameters=parameters)
#         self.params["key"] = "valid_range"


# class CheckValidMax(CheckMax):
#     """Check that no values for the specified variable are greater than
#     the maximum vaue set by the 'valid_range' attribute.  If the
#     variable in question does not posess the 'valid_range' attribute,
#     this check will be skipped.
#     """

#     def __init__(
#         self,
#         ds: xr.Dataset,
#         definition: ManagerConfig,
#         parameters: Dict[str, Any],
#     ):
#         super().__init__(ds, definition, parameters=parameters)
#         self.params["key"] = "valid_range"


# class CheckFailMin(CheckMin):
#     """Check that no values for the specified variable are less than
#     the minimum vaue set by the 'fail_range' attribute.  If the
#     variable in question does not posess the 'fail_range' attribute,
#     this check will be skipped.
#     """

#     def __init__(
#         self,
#         ds: xr.Dataset,
#         definition: ManagerConfig,
#         parameters: Dict[str, Any],
#     ):
#         super().__init__(ds, definition, parameters=parameters)
#         self.params["key"] = "fail_range"


# class CheckFailMax(CheckMax):
#     """Check that no values for the specified variable greater less than
#     the maximum vaue set by the 'fail_range' attribute.  If the
#     variable in question does not posess the 'fail_range' attribute,
#     this check will be skipped.
#     """

#     def __init__(
#         self,
#         ds: xr.Dataset,
#         definition: ManagerConfig,
#         parameters: Dict[str, Any],
#     ):
#         super().__init__(ds, definition, parameters=parameters)
#         self.params["key"] = "fail_range"


# class CheckWarnMin(CheckMin):
#     """Check that no values for the specified variable are less than
#     the minimum vaue set by the 'warn_range' attribute.  If the
#     variable in question does not posess the 'warn_range' attribute,
#     this check will be skipped.
#     """

#     def __init__(
#         self,
#         ds: xr.Dataset,
#         definition: ManagerConfig,
#         parameters: Dict[str, Any],
#     ):
#         super().__init__(ds, definition, parameters=parameters)
#         self.params["key"] = "warn_range"


# class CheckWarnMax(CheckMax):
#     """Check that no values for the specified variable are greater than
#     the maximum vaue set by the 'warn_range' attribute.  If the
#     variable in question does not posess the 'warn_range' attribute,
#     this check will be skipped.
#     """

#     def __init__(
#         self,
#         ds: xr.Dataset,
#         definition: ManagerConfig,
#         parameters: Dict[str, Any],
#     ):
#         super().__init__(ds, definition, parameters=parameters)
#         self.params["key"] = "warn_range"


# class CheckValidDelta(QualityChecker):
#     """Check that the difference between any two consecutive
#     values is not greater than the threshold set by the
#     'valid_delta' attribute.  If the variable in question
#     does not posess the 'valid_delta' attribute, this check will be skipped.
#     """

#     def run(self, variable_name: str) -> Optional[NDArray[Any]]:

#         valid_delta = self.ds[variable_name].attrs.get("valid_delta", None)

#         # If no valid_delta is available, then we just skip this definition
#         results_array: Optional[NDArray[Any]] = None
#         if valid_delta is not None:
#             # We need to get the dim to diff on from the parameters
#             # If dim is not specified, then we use the first dim for the variable
#             dim = self.params.get("dim", None)

#             if dim is None and len(self.ds[variable_name].dims) > 0:
#                 dim = self.ds[variable_name].dims[0]

#             if dim is not None:
#                 # convert to np array
#                 variable_data = self.ds[variable_name].data
#                 axis = self.ds[variable_name].get_axis_num(dim)

#                 # If the variable is a time variable, then we convert to nanoseconds before doing our check
#                 if self.ds[variable_name].data.dtype.type == np.datetime64:
#                     variable_data = utils.datetime64_to_timestamp(variable_data)

#                 # Compute the difference between each two numbers and check if it exceeds valid_delta
#                 diff = np.absolute(np.diff(variable_data, axis=axis))  # type: ignore
#                 results_array = np.greater(diff, valid_delta)

#                 # Our results array is missing one value for the first row, which is not
#                 # included in the diff computation. We add False for the first row of
#                 # results, since it won't fail the check.
#                 first_row = np.zeros(results_array[0].size, dtype=bool)  # type: ignore
#                 results_array = np.insert(results_array, 0, first_row, axis=axis)  # type: ignore

#         return results_array


# TODO: Other checks we might implement
# check_outlier(std_dev)
# check_time_gap --> parameters: min_time_gap (str), max_time_gap (str)
