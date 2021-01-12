import abc
from typing import Dict, Optional

import numpy as np
import xarray as xr

from tsdat.config import QCTestDefinition
from tsdat.constants import ATTS
from tsdat.utils import DSUtil


class QCOperator(abc.ABC):
    """-------------------------------------------------------------------
    Class containing the code to perform a single QC test on a Dataset
    variable.
    -------------------------------------------------------------------"""
    def __init__(self, ds: xr.Dataset, previous_data: xr.Dataset, test: QCTestDefinition, params: Dict):
        """-------------------------------------------------------------------
        Args:
            ds (xr.Dataset): The dataset the operator will be applied to
            previous_data (xr.Dataset): Data from the previous processing interval
            test (QCTestDefinition)  : The test definition
            params(Dict)   : A dictionary of operator-specific parameters
        -------------------------------------------------------------------"""
        self.ds = ds
        self.previous_data = previous_data
        self.test = test
        self.params = params

    @abc.abstractmethod
    def run(self, variable_name: str) -> Optional[np.ndarray]:
        """-------------------------------------------------------------------
        Test a dataset's variable to see if it passes a quality check.
        These tests can be performed on the entire variable at one time by
        using xarray vectorized numerical operators.

        Args:
            variable_name (str):  The name of the variable to check

        Returns:
            np.ndarray | None: If the test was performed, return a
            ndarray of the same shape as the variable. Each value in the
            data array will be either True or False, depending upon the
            results of the test.  True means the test failed.  False means
            it succeeded.

            Note that we are using an np.ndarray instead of an xr.DataArray
            because the DataArray contains coordinate indexes which can
            sometimes get out of sync when performing np arithmectic vector
            operations.  So it's easier to just use numpy arrays.

            If the test was skipped for some reason (i.e., it was not
            relevant given the current attributes defined for this dataset),
            then the run method should return None.
        -------------------------------------------------------------------"""
        pass


class CheckMissing(QCOperator):

    def run(self, variable_name: str) -> Optional[np.ndarray]:
        """-------------------------------------------------------------------
        Checks if any values are assigned to _FillValue or NaN
        -------------------------------------------------------------------"""
        fill_value = DSUtil.get_fill_value(self.ds, variable_name)

        # If the variable has no _FillValue attribute, then
        # we select a default value to use
        if fill_value is None:
            fill_value = -9999

        # Make sure fill value has same data type as the variable
        fill_value = np.array(fill_value, dtype=self.ds[variable_name].values.dtype.type)

        # First check if any values are assigned to _FillValue
        results_array = np.equal(self.ds[variable_name].values, fill_value)

        # Then, if the value is numeric, we should also check if any values are assigned to
        # NaN
        if self.ds[variable_name].values.dtype.type in (type(0.0), np.float16, np.float32, np.float64):
            nan = float('nan')
            nan = np.array(nan, dtype=self.ds[variable_name].values.dtype.type)
            results_array = results_array | np.equal(self.ds[variable_name].values, nan)

        return results_array


class CheckFailMin(QCOperator):

    def run(self, variable_name: str) -> Optional[np.ndarray]:
        fail_min = DSUtil.get_fail_min(self.ds, variable_name)

        # If no valid_min is available, then we just skip this test
        results_array = None
        if fail_min is not None:
            results_array = np.less(self.ds[variable_name].values, fail_min)

        return results_array


class CheckFailMax(QCOperator):

    def run(self, variable_name: str) -> Optional[np.ndarray]:
        fail_max = DSUtil.get_fail_max(self.ds, variable_name)

        # If no valid_min is available, then we just skip this test
        results_array = None
        if fail_max is not None:
            results_array = np.greater(self.ds[variable_name].values, fail_max)

        return results_array


class CheckWarnMin(QCOperator):

    def run(self, variable_name: str) -> Optional[np.ndarray]:
        warn_min = DSUtil.get_warn_min(self.ds, variable_name)

        # If no valid_min is available, then we just skip this test
        results_array = None
        if warn_min is not None:
            results_array = np.less(self.ds[variable_name].values, warn_min)

        return results_array


class CheckWarnMax(QCOperator):

    def run(self, variable_name: str) -> Optional[np.ndarray]:
        warn_max = DSUtil.get_warn_max(self.ds, variable_name)

        # If no valid_min is available, then we just skip this test
        results_array = None
        if warn_max is not None:
            results_array = np.greater(self.ds[variable_name].values, warn_max)

        return results_array


class CheckValidDelta(QCOperator):

    def run(self, variable_name: str) -> Optional[np.ndarray]:

        valid_delta = self.ds[variable_name].attrs.get(ATTS.VALID_DELTA, None)

        # If no valid_delta is available, then we just skip this test
        results_array = None
        if valid_delta is not None:
            # We need to get the dim to diff on from the parameters
            # If dim is not specified, then we use the first dim for the variable
            dim = self.params.get('dim', None)

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

                if previous_row is None:
                    # just use the first row of the current data as the previous row
                    previous_row = variable_data[0]

                # Insert that value as the first value of the first axis
                variable_data = np.insert(variable_data, 0, previous_row, axis=axis)

                diff = np.absolute(np.diff(variable_data, axis=axis))
                results_array = np.greater(diff, valid_delta)

        return results_array


# class CheckMonotonic(QCOperator):
#     def __init__(self, tsds: TimeSeriesDataset, params: Dict):
#         super().__init__(tsds, params)
#         direction = params.get('direction', 'increasing')
#         self.increasing = False
#         if direction == 'increasing':
#             self.increasing = True
#         self.interval = params.get('interval', None)
#         self.interval = abs(self.interval) # make sure it's a positive number
#
#     def run(self, variable_name: str, coordinates: List[int], value: Any):
#
#         # If current value is missing, skip this test
#         if not self.tsds.is_missing(variable_name, value):
#             # Get the axis to navigate on - by default we will use axis 0 (i.e., x in x,y,z)
#             # TODO: the axis dimension for this check should be overridable in the config
#             axis = 0
#
#             # Get the previous value
#             prev_value = self.tsds.get_previous_value(variable_name, coordinates, axis)
#
#             # If there is no previous value or previous value is missing, skip this test
#             # TODO: do we need to navigate backward until we find a non-missing value?
#             # Not sure what the typical logic is for this type of function
#             if prev_value is not None and not self.tsds.is_missing(variable_name, prev_value):
#
#                 # If this variable is datetime64, then convert the value to the timestamp
#                 # in order to do this check.
#                 if type(value) == np.datetime64:
#                     value = TimeSeriesDataset.get_timestamp(value)
#                     prev_value = TimeSeriesDataset.get_timestamp(prev_value)
#
#                 if self.increasing:
#                     delta = value - prev_value
#                 else:
#                     delta = prev_value - value
#
#                 if self.interval and delta != self.interval:
#                     return False
#
#                 elif not self.interval and delta <= 0:
#                     return False
#
#         return True



# TODO: Other tests we might implement
# check_outlier(std_dev)



