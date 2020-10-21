from abc import abstractmethod
import numpy as np
from typing import List, Dict, Any
from tsdat import TimeSeriesDataset


class QCOperator:
    def __init__(self, tsds: TimeSeriesDataset, params: Dict):
        self.tsds = tsds
        self.params = params

    @abstractmethod
    def run(self, variable_name: str, coordinates: List[int], value: Any):
        """
        Test a variable's data value and see if it passes a quality check.

        :param variable_name: Name of the variable to check
        :param coordinates: n-dimensional data index of the value (i.e., [1246, 1] for [time, height]
        This is passed in case the operator needs to check other variable values at the same index.
        :param value: The value to test
        :return: True if the test passed, False if it failed
        :rtype: bool
        """
        pass


class CheckMissing(QCOperator):

    def run(self, variable_name: str, coordinates: List[int], value: Any):
        test_passed = not self.tsds.is_missing(variable_name, value)
        return test_passed


class CheckValidMin(QCOperator):

    def run(self, variable_name: str, coordinates: List[int], value: Any):
        # Get the variable's valid_min
        ds_var = self.tsds.get_var(variable_name)
        valid_min = ds_var.attrs.get(TimeSeriesDataset.ATTS.VALID_MIN)

        # Only run test if there is a valid_min defined and current value is not missing_value
        if valid_min and not self.tsds.is_missing(variable_name, value):
            if value < valid_min:
                return False

        return True


class CheckValidMax(QCOperator):

    def run(self, variable_name: str, coordinates: List[int], value: Any):
        # Get the variable's valid_max
        ds_var = self.tsds.get_var(variable_name)
        valid_max = ds_var.attrs.get(TimeSeriesDataset.ATTS.VALID_MAX)

        # Only run test if there is a valid_max defined and current value is not missing_value
        if valid_max and not self.tsds.is_missing(variable_name, value):
            if value > valid_max:
                return False

        return True


class CheckValidDelta(QCOperator):

    def run(self, variable_name: str, coordinates: List[int], value: Any):
        # Get the variable's valid_delta
        ds_var = self.tsds.get_var(variable_name)
        valid_delta = ds_var.attrs.get(TimeSeriesDataset.ATTS.VALID_DELTA)

        # If there is no valid_delta value or current value is missing, skip this test
        if valid_delta and not self.tsds.is_missing(variable_name, value):
            # Get the axis to navigate on - by default we will use axis 0 (i.e., x in x,y,z)
            # TODO: the axis dimension for the delta check should be overridable in the config
            axis = 0

            # Get the previous value
            prev_value = self.tsds.get_previous_value(variable_name, coordinates, axis)

            # If there is no previous value or previous value is missing, skip this test
            # TODO: do we need to navigate backward until we find a non-missing value?
            # Not sure what the typical logic is for this type of function
            if prev_value is not None and not self.tsds.is_missing(variable_name, prev_value):
                delta = abs(value - prev_value)
                if delta > valid_delta:
                    return False

        return True


class CheckMonotonic(QCOperator):
    def __init__(self, tsds: TimeSeriesDataset, params: Dict):
        super().__init__(tsds, params)
        direction = params.get('direction', 'increasing')
        self.increasing = False
        if direction == 'increasing':
            self.increasing = True
        self.interval = params.get('interval', None)
        self.interval = abs(self.interval) # make sure it's a positive number

    def run(self, variable_name: str, coordinates: List[int], value: Any):

        # If current value is missing, skip this test
        if not self.tsds.is_missing(variable_name, value):
            # Get the axis to navigate on - by default we will use axis 0 (i.e., x in x,y,z)
            # TODO: the axis dimension for this check should be overridable in the config
            axis = 0

            # Get the previous value
            prev_value = self.tsds.get_previous_value(variable_name, coordinates, axis)

            # If there is no previous value or previous value is missing, skip this test
            # TODO: do we need to navigate backward until we find a non-missing value?
            # Not sure what the typical logic is for this type of function
            if prev_value is not None and not self.tsds.is_missing(variable_name, prev_value):

                # If this variable is datetime64, then convert the value to the timestamp
                # in order to do this check.
                if type(value) == np.datetime64:
                    value = TimeSeriesDataset.get_timestamp(value)
                    prev_value = TimeSeriesDataset.get_timestamp(prev_value)

                if self.increasing:
                    delta = value - prev_value
                else:
                    delta = prev_value - value

                if self.interval and delta != self.interval:
                    return False

                elif not self.interval and delta <= 0:
                    return False

        return True



# TODO:
# check_inf
# check_nan
# check_outlier(std_dev)
# - tsdat.qc.operators.CheckType (not sure if this is realistic)



