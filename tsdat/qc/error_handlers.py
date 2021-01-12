import abc
from typing import List, Dict, Any
import xarray as xr
from tsdat.config import QCTestDefinition


class QCErrorHandler(abc.ABC):
    def __init__(self, ds: xr.Dataset, previous_data: xr.Dataset, test: QCTestDefinition, params: Dict):
        """-------------------------------------------------------------------
        Args:
            ds (xr.Dataset): The dataset the operator will be applied to
            test (QCTestDefinition)  : The test definition
            params(Dict)   : A dictionary of operator-specific parameters
        -------------------------------------------------------------------"""
        self.ds = ds
        self.previous_data = previous_data
        self.test = test
        self.params = params

    @abc.abstractmethod
    def run(self, variable_name: str, coordinates: List[int]):
        """-------------------------------------------------------------------
        Perform a follow-on action if a qc test fails.  This can be used to
        correct data if needed (such as replacing a bad value with missing value,
        emailing a contact persion, or raising an exception if the failure
        constitutes a critical error).

        Args:
            variable_name (str): Name of the variable that failed
            test (QCTestDefinition)  : The test definition
            params(Dict)   : A dictionary of operator-specific parameters
        -------------------------------------------------------------------"""
        """
        Perform a follow-on action if a qc test fails

        :param variable_name: Name of the variable that failed
        :param coordinates: n-dimensional data index of the value that failed (i.e., [1246, 1] for [time, height]
        """
        pass


class ReplaceMissing(QCErrorHandler):

    def run(self, variable_name: str, coordinates: List[int]):
        # Set the value at the given coordinates to missing value
        missing_value = self.tsds.get_missing_value(variable_name)
        var = self.tsds.xr.get(variable_name)

        if len(coordinates) == 1:
            x = coordinates[0]
            var.values[x] = missing_value

        elif len(coordinates) == 2:
            x = coordinates[0]
            y = coordinates[1]
            var.values[x][y] = missing_value

        elif len(coordinates) == 3:
            x = coordinates[0]
            y = coordinates[1]
            z = coordinates[2]
            var.values[x][y][z] = missing_value


# TODO: possible other error handlers
# tsdat.qc.error_handlers.Fail  # fail the pipeline
# TODO: what about an email handler?
