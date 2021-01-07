import abc
import numpy as np
from typing import List, Dict, Any
import xarray as xr
from tsdat import QCTest
from tsdat.data_model.atts import ATTS


class QCOperator(abc.ABC):
    """-------------------------------------------------------------------
    Class containing the code to perform a single QC test on a Dataset
    variable.
    -------------------------------------------------------------------"""
    def __init__(self, ds: xr.Dataset, test: QCTest, params: Dict):
        """-------------------------------------------------------------------
        Args:
            ds (xr.Dataset): The dataset the operator will be applied to
            test (QCTest)  : The test definition
            params(Dict)   : A dictionary of operator-specific parameters
        -------------------------------------------------------------------"""
        self.ds = ds
        self.test = test
        self.params = params

    @abc.abstractmethod
    def run(self, variable_name: str):
        """-------------------------------------------------------------------
        Test a dataset's variable to see if it passes a quality check.
        These tests can be performed on the entire variable at one time by
        using xarray vectorized numerical operators.

        Args:
            variable_name (str):  The name of the variable to check

        Returns:
            xr.DataArray | None: If the test was performed, return a
            DataArray of the same shape as the variable. Each value in the
            data array will be either True or False, depending upon the
            results of the test.  True means the test failed.  False means
            it succeeded.

            If the test was skipped for some reason (i.e., it was not
            relevant given the current attributes defined for this dataset),
            then the run method should return None.
        -------------------------------------------------------------------"""
        pass


class CheckMissing(QCOperator):

    def run(self, variable_name: str):
        missing_value = ATTS.get_missing_value(variable_name)

        # If the variable has no _FillValue or missing_value attribute, then
        # we select a default value to use
        if (missing_value is None and self.ds[variable_name].values.dtype.type in
                (type(0.0), np.float16, np.float32, np.float64)):
            missing_value = float('nan')
        else:
            missing_value = -9999

        # Ensure missing_value attribute is matching data type
        missing_value = np.array(missing_value, dtype=self.ds[variable_name].values.dtype.type)
        results_array = np.equal(self.ds[variable_name].values, missing_value)
        return results_array


class CheckValidMin(QCOperator):

    def run(self, variable_name: str):
        # Get the correct limit to use depending upon the test assessment
        valid_min = None
        if self.test.assessment == QCTest.BAD:
            valid_min = ATTS.get_fail_min(self.ds, variable_name)

        elif self.test.assessment == QCTest.INDETERMINATE:
            valid_min = ATTS.get_warn_min(self.ds, variable_name)

        # If no valid_min is available, then we just skip this test
        results_array = None
        if valid_min is not None:
            results_array = np.less(self.ds[variable_name].values, valid_min)

        return results_array


class CheckValidMax(QCOperator):

    def run(self, variable_name: str):
        # Get the correct limit to use depending upon the test assessment
        valid_max = None
        if self.test.assessment == QCTest.BAD:
            valid_max = ATTS.get_fail_max(self.ds, variable_name)

        elif self.test.assessment == QCTest.INDETERMINATE:
            valid_max = ATTS.get_warn_max(self.ds, variable_name)

        # If no valid_min is available, then we just skip this test
        results_array = None
        if valid_max is not None:
            results_array = np.greater(self.ds[variable_name].values, valid_max)

        return results_array


class CheckValidDelta(QCOperator):

    def run(self, variable_name: str):

        valid_delta = ATTS.get_valid_delta(variable_name)

        # If no valid_delta is available, then we just skip this test
        results_array = None
        if valid_delta is not None:
            # We need to get the dim to diff on from the parameters
            # If dim is not specified, then we use the first dim for the variable
            dim = self.params.get('dim', None)

            if dim is None and len(self.ds[variable_name].dims) > 0:
                dim = self.ds[variable_name].dims[0]

            if dim is not None:
                diff = self.ds[variable_name].diff("time")

                # Pad the diff array with a good value since it is one less than the length of dim
                diff = np.insert(diff, 0, valid_delta)

                results_array = np.logical_not(np.less_equal(diff, valid_delta))

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



# TODO:
# check_inf
# check_nan
# check_outlier(std_dev)
# - tsdat.qc.operators.CheckType (not sure if this is realistic)



