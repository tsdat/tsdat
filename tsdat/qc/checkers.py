import abc
import numpy as np
import xarray as xr
from typing import List, Optional, Dict, Union

from tsdat.config import QualityManagerDefinition
from tsdat.constants import ATTS
from tsdat.utils import DSUtil


class QualityChecker(abc.ABC):
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
    :type definition: QualityManagerDefinition
    :param parameters: A dictionary of checker-specific parameters specified in the
        pipeline config file.  Defaults to {}
    :type parameters: dict, optional
    """

    def __init__(
        self,
        ds: xr.Dataset,
        previous_data: xr.Dataset,
        definition: QualityManagerDefinition,
        parameters: Union[Dict, None] = None,
    ):

        self.ds = ds
        self.previous_data = previous_data
        self.definition = definition
        self.params = parameters if parameters is not None else dict()

    @abc.abstractmethod
    def run(self, variable_name: str) -> Optional[np.ndarray]:
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

            Note that we are using an np.ndarray instead of an xr.DataArray
            because the DataArray contains coordinate indexes which can
            sometimes get out of sync when performing np arithmectic vector
            operations.  So it's easier to just use numpy arrays.

            If the check was skipped for some reason (i.e., it was not
            relevant given the current attributes defined for this dataset),
            then the run method should return None.
        :rtype: Optional[np.ndarray]
        """
        pass


class CheckMissing(QualityChecker):
    """Checks if any values are assigned to _FillValue or 'NaN' (for non-time
    variables) or checks if values are assigned to 'NaT' (for time variables).
    Also, for non-time variables, checks if values are above or below
    valid_range, as this is considered missing as well.
    """

    def run(self, variable_name: str) -> Optional[np.ndarray]:

        # If this is a time variable, we check for 'NaT'
        if self.ds[variable_name].data.dtype.type == np.datetime64:
            results_array = np.isnat(self.ds[variable_name].data)

        else:
            fill_value = DSUtil.get_fill_value(self.ds, variable_name)

            # If the variable has no _FillValue attribute, then
            # we select a default value to use
            if fill_value is None:
                fill_value = -9999

            # Make sure fill value has same data type as the variable
            fill_value = np.array(
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


class CheckMin(QualityChecker):
    """Check that no values for the specified variable are less than
    a specified minimum threshold.  The threshold value is an attribute
    set on the variable in question. The  attribute name is
    specified in the quality checker definition in the pipeline config
    file by setting a param called 'key: ATTRIBUTE_NAME'.

    If the key parameter is not set or the variable does not possess
    the specified attribute, this check will be skipped.
    """

    def run(self, variable_name: str) -> Optional[np.ndarray]:
        # Get the minimum value
        _min = self.ds[variable_name].attrs.get(self.params["key"], None)
        if isinstance(_min, List):
            _min = _min[0]

        # If no minimum value is available, then we just skip this check
        results_array = None
        if _min is not None:
            results_array = np.less(self.ds[variable_name].data, _min)

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

    def run(self, variable_name: str) -> Optional[np.ndarray]:
        # Get the maximum value
        _max = self.ds[variable_name].attrs.get(self.params["key"], None)
        if isinstance(_max, List):
            _max = _max[-1]

        # If no maximum value is available, then we just skip this check
        results_array = None
        if _max is not None:
            results_array = np.greater(self.ds[variable_name].data, _max)

        return results_array


class CheckValidMin(CheckMin):
    """Check that no values for the specified variable are less than
    the minimum vaue set by the 'valid_range' attribute.  If the
    variable in question does not posess the 'valid_range' attribute,
    this check will be skipped.
    """

    def __init__(
        self,
        ds: xr.Dataset,
        previous_data: xr.Dataset,
        definition: QualityManagerDefinition,
        parameters,
    ):
        super().__init__(ds, previous_data, definition, parameters=parameters)
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
        previous_data: xr.Dataset,
        definition: QualityManagerDefinition,
        parameters,
    ):
        super().__init__(ds, previous_data, definition, parameters=parameters)
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
        previous_data: xr.Dataset,
        definition: QualityManagerDefinition,
        parameters,
    ):
        super().__init__(ds, previous_data, definition, parameters=parameters)
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
        previous_data: xr.Dataset,
        definition: QualityManagerDefinition,
        parameters,
    ):
        super().__init__(ds, previous_data, definition, parameters=parameters)
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
        previous_data: xr.Dataset,
        definition: QualityManagerDefinition,
        parameters,
    ):
        super().__init__(ds, previous_data, definition, parameters=parameters)
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
        previous_data: xr.Dataset,
        definition: QualityManagerDefinition,
        parameters,
    ):
        super().__init__(ds, previous_data, definition, parameters=parameters)
        self.params["key"] = "warn_range"


class CheckValidDelta(QualityChecker):
    """Check that the difference between any two consecutive
    values is not greater than the threshold set by the
    'valid_delta' attribute.  If the variable in question
    does not posess the 'valid_delta' attribute, this check will be skipped.
    """

    def run(self, variable_name: str) -> Optional[np.ndarray]:

        valid_delta = self.ds[variable_name].attrs.get(ATTS.VALID_DELTA, None)

        # If no valid_delta is available, then we just skip this definition
        results_array = None
        if valid_delta is not None:
            # We need to get the dim to diff on from the parameters
            # If dim is not specified, then we use the first dim for the variable
            dim = self.params.get("dim", None)

            if dim is None and len(self.ds[variable_name].dims) > 0:
                dim = self.ds[variable_name].dims[0]

            if dim is not None:
                # If previous data exists, then we must add the last row of
                # previous data as the first row of the variable's data array.
                # This is so that the diff function can compare the first value
                # of the file to make sure it is consistent with the previous file.

                # convert to np array
                variable_data = self.ds[variable_name].data
                axis = self.ds[variable_name].get_axis_num(dim)
                previous_row = None

                # Load the previous row from the other dataset
                if self.previous_data is not None:
                    previous_variable_data = self.previous_data.get(variable_name, None)
                    if previous_variable_data is not None:
                        # convert to np array
                        previous_variable_data = previous_variable_data.data

                        # Get the last value from the first axis
                        previous_row = previous_variable_data[-1]

                        # Insert that value as the first value of the first axis
                        variable_data = np.insert(
                            variable_data, 0, previous_row, axis=axis
                        )

                # If the variable is a time variable, then we convert to nanoseconds before doing our check
                if self.ds[variable_name].data.dtype.type == np.datetime64:
                    variable_data = DSUtil.datetime64_to_timestamp(variable_data)

                # Compute the difference between each two numbers and check if it exceeds valid_delta
                diff = np.absolute(np.diff(variable_data, axis=axis))
                results_array = np.greater(diff, valid_delta)

                if previous_row is None:
                    # This means our results array is missing one value for the first row, which is
                    # not included in the diff computation.
                    # We need to add False for the first row of results, since it won't fail
                    # the check.
                    first_row = np.zeros(results_array[0].size, dtype=bool)
                    results_array = np.insert(results_array, 0, first_row, axis=axis)

        return results_array


class CheckMonotonic(QualityChecker):
    """Checks that all values for the specified variable are either
    strictly increasing or strictly decreasing.
    """

    def run(self, variable_name: str) -> Optional[np.ndarray]:

        results_array = None
        # We need to get the dim to diff on from the parameters
        # If dim is not specified, then we use the first dim for the variable
        dim = self.params.get("dim", None)

        if dim is None and len(self.ds[variable_name].dims) > 0:
            dim = self.ds[variable_name].dims[0]

        if dim is not None:
            # If previous data exists, then we must add the last row of
            # previous data as the first row of the variable's data array.
            # This is so that the diff function can compare the first value
            # of the file to make sure it is consistent with the previous file.

            # convert to np array
            variable_data = self.ds[variable_name].data
            axis = self.ds[variable_name].get_axis_num(dim)
            previous_row = None

            # Load the previous row from the other dataset
            if self.previous_data is not None and dim == "time":
                previous_variable_data = self.previous_data.get(variable_name, None)
                if previous_variable_data is not None:
                    # convert to np array
                    previous_variable_data = previous_variable_data.data

                    # Get the last value from the first axis
                    previous_row = previous_variable_data[-1]

                    # Insert that value as the first value of the first axis
                    variable_data = np.insert(variable_data, 0, previous_row, axis=axis)

            # If the variable is a time variable, then we convert to nanoseconds before doing our check
            if self.ds[variable_name].data.dtype.type == np.datetime64:
                variable_data = DSUtil.datetime64_to_timestamp(variable_data)

            # Compute the difference between each two numbers and check if they are either all
            # increasing or all decreasing
            diff = np.diff(variable_data, axis=axis)
            is_monotonic = np.all(diff > 0) | np.all(diff < 0)  # this returns a scalar

            # Create a results array, with all values set to the results of the is_monotonic check
            results_array = np.full(variable_data.shape, not is_monotonic, dtype=bool)

        return results_array


# TODO: Other checks we might implement
# check_outlier(std_dev)
