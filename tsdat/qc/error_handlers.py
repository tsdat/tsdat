import abc

import numpy as np
import xarray as xr

from tsdat.config import QualityManagerDefinition
from tsdat.exceptions import QCError
from tsdat.utils import DSUtil

class QCParamKeys:
    QC_BIT = 'bit'
    ASSESSMENT = 'assessment'
    TEST_MEANING = 'meaning'
    CORRECTION = 'correction'


class QualityHandler(abc.ABC):
    """-------------------------------------------------------------------
    Class containing code to be executed if a particular qc test fails.
    -------------------------------------------------------------------"""

    def __init__(self, ds: xr.Dataset, previous_data: xr.Dataset, test: QualityManagerDefinition, parameters={}):
        """-------------------------------------------------------------------
        Args:
            ds (xr.Dataset): The dataset the operator will be applied to
            test (QCTestDefinition)  : The test definition
            params(Dict)   : A dictionary of handler-specific parameters
        -------------------------------------------------------------------"""
        self.ds = ds
        self.previous_data = previous_data
        self.test = test
        self.params = parameters


    @abc.abstractmethod
    def run(self, variable_name: str, results_array: np.ndarray):
        """-------------------------------------------------------------------
        Perform a follow-on action if a qc test fails.  This can be used to
        correct data if needed (such as replacing a bad value with missing value,
        emailing a contact persion, or raising an exception if the failure
        constitutes a critical error).

        Args:
            variable_name (str): Name of the variable that failed
            results_array (np.ndarray)  : An array of True/False values for
            each data value of the variable.  True means the test failed.
        -------------------------------------------------------------------"""
        pass

    def record_correction(self, variable_name: str):
        correction = self.params.get("correction", None)
        if correction is not None:
            DSUtil.record_corrections_applied(self.ds, variable_name, correction)


class RecordQualityResults(QualityHandler):
    """-----------------------------------------------------------------------
    Record the results of the qc test in an ancillary qc variable.
    -----------------------------------------------------------------------"""
    def run(self, variable_name: str, results_array: np.ndarray):
        self.ds.qcfilter.add_test(
            variable_name, index=results_array,
            test_number=self.params.get(QCParamKeys.QC_BIT),
            test_meaning=self.params.get(QCParamKeys.TEST_MEANING),
            test_assessment=self.params.get(QCParamKeys.ASSESSMENT))


class RemoveFailedValues(QualityHandler):
    """-------------------------------------------------------------------
    Replace all the failed values with _FillValue
    -------------------------------------------------------------------"""
    def run(self, variable_name: str, results_array: np.ndarray):
        if results_array.any():
            fill_value = DSUtil.get_fill_value(self.ds, variable_name)
            keep_array = np.logical_not(results_array)

            var_values = self.ds[variable_name].data
            replaced_values = np.where(keep_array, var_values, fill_value)
            self.ds[variable_name].data = replaced_values

            self.record_correction(variable_name)


class SendEmailAWS(QualityHandler):
    """-------------------------------------------------------------------
    Send an email to the recipients using AWS services.
    -------------------------------------------------------------------"""
    def run(self, variable_name: str, results_array: np.ndarray):
        # TODO: we will implement this later after we get the cloud
        # stuff implemented.
        pass


class FailPipeline(QualityHandler):
    """-------------------------------------------------------------------
    Throw an exception, halting the pipeline & indicating a critical error
    -------------------------------------------------------------------"""
    def run(self, variable_name: str, results_array: np.ndarray):
        if results_array.any():
            message = f"QC test {self.test.name} failed for variable {variable_name}"
            raise QCError(message)
