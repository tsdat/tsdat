import numpy as np
import xarray as xr
from abc import ABC, abstractmethod
from numpy.typing import NDArray
from typing import Any, List, Optional, Dict, Union

from tsdat import utils
from tsdat.config.quality import ManagerConfig


class QualityChecker(ABC):
    """Class containing the code to perform a single Quality Check on a
    Dataset variable.

    :param ds: The dataset the checker will be applied to
    :type ds: xr.Dataset
    :param previous_data: A dataset from the previous processing interval
        (i.e., file).  This is used to check for consistency between files,
        such as for monitonic or delta checks when we need to check the previous value.
    :type previous_data: xr.Dataset
    :param definition: The quality manager definition as specified in the
        pipeline config file
    :type definition: ManagerConfig
    :param parameters: A dictionary of checker-specific parameters specified in the
        pipeline config file.  Defaults to {}
    :type parameters: dict, optional
    """

    # TODO: Add a 'name' parameter here (useful for logging / debugging)
    def __init__(
        self,
        ds: xr.Dataset,
        definition: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        self.ds: xr.Dataset = ds
        self.definition: ManagerConfig = definition
        self.params: Dict[str, Any] = parameters

    @abstractmethod
    def run(self, variable_name: str) -> Optional[NDArray[Any]]:
        """Check a dataset's variable to see if it passes a quality check.
        These checks can be performed on the entire variable at one time by
        using xarray vectorized numerical operators.

        :param variable_name: The name of the variable to check
        :type variable_name: str
        :return: If the check was performed, return a
            ndarray of the same shape as the variable. Each value in the
            data array will be either True or False, depending upon the
            results of the check.  True means the check failed.  False means
            it succeeded.

            Note that we are using an NDArray[Any] instead of an xr.DataArray
            because the DataArray contains coordinate indexes which can
            sometimes get out of sync when performing np arithmectic vector
            operations.  So it's easier to just use numpy arrays.

            If the check was skipped for some reason (i.e., it was not
            relevant given the current attributes defined for this dataset),
            then the run method should return None.
        :rtype: Optional[NDArray[Any]]
        """
        pass


class CheckMissing(QualityChecker):
    """Checks if any values are assigned to _FillValue or 'NaN' (for non-time
    variables) or checks if values are assigned to 'NaT' (for time variables).
    Also, for non-time variables, checks if values are above or below
    valid_range, as this is considered missing as well.
    """

    def run(self, variable_name: str) -> NDArray[Any]:

        # If this is a time variable, we check for 'NaT'
        if self.ds[variable_name].data.dtype.type == np.datetime64:
            results_array = np.isnat(self.ds[variable_name].data)

        else:
            # HACK: until we centralize / construct logic for this
            fill_value = self.ds[variable_name].attrs["_FillValue"]

            # If the variable has no _FillValue attribute, then
            # we select a default value to use
            if fill_value is None:
                fill_value = -9999

            # Make sure fill value has same data type as the variable
            fill_value = np.array(  # type: ignore
                fill_value, dtype=self.ds[variable_name].data.dtype.type
            )

            # First check if any values are assigned to _FillValue
            results_array = np.equal(self.ds[variable_name].data, fill_value)

            # Then, if the value is numeric, we should also check if any values are assigned to NaN
            if self.ds[variable_name].data.dtype.type in (
                type(0.0),
                np.float16,
                np.float32,
                np.float64,
            ):
                results_array |= np.isnan(self.ds[variable_name].data)

        return results_array


class CheckMonotonic(QualityChecker):
    """Checks that all values for the specified variable are either
    strictly increasing or strictly decreasing.
    """

    def run(self, variable_name: str) -> Optional[NDArray[Any]]:

        results_array: Optional[NDArray[Any]] = None
        # We need to get the dim to diff on from the parameters
        # If dim is not specified, then we use the first dim for the variable
        dim = self.params.get("dim", None)

        if dim is None and len(self.ds[variable_name].dims) > 0:
            dim = self.ds[variable_name].dims[0]

        if dim is not None:

            # convert to np array
            variable_data = self.ds[variable_name].data
            axis = self.ds[variable_name].get_axis_num(dim)

            # If the variable is a time variable, then we convert to nanoseconds before
            # doing our check.
            if self.ds[variable_name].data.dtype.type == np.datetime64:
                variable_data = utils.datetime64_to_timestamp(variable_data)

            # Compute the difference between each two numbers and check if they are
            # either all increasing or all decreasing
            diff: NDArray[Any] = np.diff(variable_data, axis=axis)  # type: ignore
            is_monotonic = np.all(diff > 0) | np.all(diff < 0)  # type: ignore # this returns a scalar

            # Create a results array, with all values set to the results of the is_monotonic check
            results_array = np.full(variable_data.shape, not is_monotonic, dtype=bool)  # type: ignore

        return results_array


class CheckMin(QualityChecker):
    """Check that no values for the specified variable are less than
    a specified minimum threshold.  The threshold value is an attribute
    set on the variable in question. The  attribute name is
    specified in the quality checker definition in the pipeline config
    file by setting a param called 'key: ATTRIBUTE_NAME'.

    If the key parameter is not set or the variable does not possess
    the specified attribute, this check will be skipped.
    """

    def __init__(
        self, ds: xr.Dataset, definition: ManagerConfig, parameters: Dict[str, Any]
    ):
        super().__init__(ds, definition, parameters)
        self.key: str = self.params["key"]

    def run(self, variable_name: str) -> Optional[NDArray[Any]]:
        results_array: Optional[NDArray[Any]] = None
        min_value: Optional[Union[float, List[float]]] = self.ds[
            variable_name
        ].attrs.get(self.key, None)

        if min_value is not None:
            if isinstance(min_value, List):
                min_value = min_value[0]

            results_array = np.less(self.ds[variable_name].data, min_value)

        return results_array


class CheckMax(QualityChecker):
    """Check that no values for the specified variable are greater than
    a specified maximum threshold.  The threshold value is an attribute
    set on the variable in question. The  attribute name is
    specified in the quality checker definition in the pipeline config
    file by setting a param called 'key: ATTRIBUTE_NAME'.

    If the key parameter is not set or the variable does not possess
    the specified attribute, this check will be skipped.
    """

    def __init__(
        self, ds: xr.Dataset, definition: ManagerConfig, parameters: Dict[str, Any]
    ):
        super().__init__(ds, definition, parameters)
        self.key: str = self.params["key"]

    def run(self, variable_name: str) -> Optional[NDArray[Any]]:
        results_array: Optional[NDArray[Any]] = None
        max_value: Optional[Union[float, List[float]]] = self.ds[
            variable_name
        ].attrs.get(self.key, None)

        if max_value is not None:
            if isinstance(max_value, List):
                max_value = max_value[1]

            results_array = np.greater(self.ds[variable_name].data, max_value)

        return results_array


# TODO: Initialize the remaining classes automatically


class CheckValidMin(CheckMin):
    """Check that no values for the specified variable are less than
    the minimum vaue set by the 'valid_range' attribute.  If the
    variable in question does not posess the 'valid_range' attribute,
    this check will be skipped.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        definition: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        super().__init__(ds, definition, parameters=parameters)
        self.params["key"] = "valid_range"


class CheckValidMax(CheckMax):
    """Check that no values for the specified variable are greater than
    the maximum vaue set by the 'valid_range' attribute.  If the
    variable in question does not posess the 'valid_range' attribute,
    this check will be skipped.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        definition: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        super().__init__(ds, definition, parameters=parameters)
        self.params["key"] = "valid_range"


class CheckFailMin(CheckMin):
    """Check that no values for the specified variable are less than
    the minimum vaue set by the 'fail_range' attribute.  If the
    variable in question does not posess the 'fail_range' attribute,
    this check will be skipped.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        definition: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        super().__init__(ds, definition, parameters=parameters)
        self.params["key"] = "fail_range"


class CheckFailMax(CheckMax):
    """Check that no values for the specified variable greater less than
    the maximum vaue set by the 'fail_range' attribute.  If the
    variable in question does not posess the 'fail_range' attribute,
    this check will be skipped.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        definition: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        super().__init__(ds, definition, parameters=parameters)
        self.params["key"] = "fail_range"


class CheckWarnMin(CheckMin):
    """Check that no values for the specified variable are less than
    the minimum vaue set by the 'warn_range' attribute.  If the
    variable in question does not posess the 'warn_range' attribute,
    this check will be skipped.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        definition: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        super().__init__(ds, definition, parameters=parameters)
        self.params["key"] = "warn_range"


class CheckWarnMax(CheckMax):
    """Check that no values for the specified variable are greater than
    the maximum vaue set by the 'warn_range' attribute.  If the
    variable in question does not posess the 'warn_range' attribute,
    this check will be skipped.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        definition: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        super().__init__(ds, definition, parameters=parameters)
        self.params["key"] = "warn_range"


class CheckValidDelta(QualityChecker):
    """Check that the difference between any two consecutive
    values is not greater than the threshold set by the
    'valid_delta' attribute.  If the variable in question
    does not posess the 'valid_delta' attribute, this check will be skipped.
    """

    def run(self, variable_name: str) -> Optional[NDArray[Any]]:

        valid_delta = self.ds[variable_name].attrs.get("valid_delta", None)

        # If no valid_delta is available, then we just skip this definition
        results_array: Optional[NDArray[Any]] = None
        if valid_delta is not None:
            # We need to get the dim to diff on from the parameters
            # If dim is not specified, then we use the first dim for the variable
            dim = self.params.get("dim", None)

            if dim is None and len(self.ds[variable_name].dims) > 0:
                dim = self.ds[variable_name].dims[0]

            if dim is not None:
                # convert to np array
                variable_data = self.ds[variable_name].data
                axis = self.ds[variable_name].get_axis_num(dim)

                # If the variable is a time variable, then we convert to nanoseconds before doing our check
                if self.ds[variable_name].data.dtype.type == np.datetime64:
                    variable_data = utils.datetime64_to_timestamp(variable_data)

                # Compute the difference between each two numbers and check if it exceeds valid_delta
                diff = np.absolute(np.diff(variable_data, axis=axis))  # type: ignore
                results_array = np.greater(diff, valid_delta)

                # Our results array is missing one value for the first row, which is not
                # included in the diff computation. We add False for the first row of
                # results, since it won't fail the check.
                first_row = np.zeros(results_array[0].size, dtype=bool)  # type: ignore
                results_array = np.insert(results_array, 0, first_row, axis=axis)  # type: ignore

        return results_array


# TODO: Other checks we might implement
# check_outlier(std_dev)
# check_time_gap --> parameters: min_time_gap (str), max_time_gap (str)
