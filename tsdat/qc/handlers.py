import abc
from typing import Union, Dict

import numpy as np
import xarray as xr

from tsdat.config import QualityManagerDefinition
from tsdat.exceptions import QCError
from tsdat.utils import DSUtil


class QCParamKeys:
    """Symbolic constants used for referencing QC-related
    fields in the pipeline config file
    """

    QC_BIT = "bit"
    ASSESSMENT = "assessment"
    TEST_MEANING = "meaning"
    CORRECTION = "correction"


class QualityHandler(abc.ABC):
    """Class containing code to be executed if a particular quality check fails.

    :param ds: The dataset the handler will be applied to
    :type ds: xr.Dataset
    :param previous_data: A dataset from the previous processing interval
        (i.e., file).  This is used to check for consistency between files,
        such as for monotonic or delta checks when we need to check the previous value.
    :type previous_data: xr.Dataset
    :param quality_manager: The quality_manager definition as specified in the
        pipeline config file
    :type quality_manager: QualityManagerDefinition
    :param parameters: A dictionary of handler-specific parameters specified in the
        pipeline config file.  Defaults to {}
    :type parameters: dict, optional
    """

    def __init__(
        self,
        ds: xr.Dataset,
        previous_data: xr.Dataset,
        quality_manager: QualityManagerDefinition,
        parameters: Union[Dict, None] = None,
    ):
        self.ds = ds
        self.previous_data = previous_data
        self.quality_manager = quality_manager
        self.params = parameters if parameters is not None else dict()

    @abc.abstractmethod
    def run(self, variable_name: str, results_array: np.ndarray):
        """Perform a follow-on action if a quality check fails. This can be used
        to correct data if needed (such as replacing a bad value with missing
        value, emailing a contact persion, or raising an exception if the
        failure constitutes a critical error).

        :param variable_name: Name of the variable that failed
        :type variable_name: str
        :param results_array: An array of True/False values for each data value
            of the variable.  True means the check failed.
        :type results_array: np.ndarray
        """
        pass

    def record_correction(self, variable_name: str):
        """If a correction was made to variable data to fix invalid values
        as detected by a quality check, this method will record the fix
        to the appropriate variable attribute.  The correction description
        will come from the handler params which get set in the pipeline config
        file.

        :param variable_name: Name
        :type variable_name: str
        """
        correction = self.params.get("correction", None)
        if correction is not None:
            DSUtil.record_corrections_applied(self.ds, variable_name, correction)


class RecordQualityResults(QualityHandler):
    """Record the results of the quality check in an ancillary qc variable."""

    def run(self, variable_name: str, results_array: np.ndarray):

        self.ds.qcfilter.add_test(
            variable_name,
            index=results_array,
            test_number=self.params.get(QCParamKeys.QC_BIT),
            test_meaning=self.params.get(QCParamKeys.TEST_MEANING),
            test_assessment=self.params.get(QCParamKeys.ASSESSMENT),
        )


class RemoveFailedValues(QualityHandler):
    """Replace all the failed values with _FillValue"""

    def run(self, variable_name: str, results_array: np.ndarray):
        if results_array.any():
            fill_value = DSUtil.get_fill_value(self.ds, variable_name)
            keep_array = np.logical_not(results_array)

            var_values = self.ds[variable_name].data
            replaced_values = np.where(keep_array, var_values, fill_value)
            self.ds[variable_name].data = replaced_values

            self.record_correction(variable_name)


class SortDatasetByCoordinate(QualityHandler):
    """Sort coordinate data using xr.Dataset.sortby(). Accepts the following
    parameters:

    .. code-block:: yaml

        parameters:
          # Whether or not to sort in ascending order. Defaults to True.
          ascending: True
    """

    def run(self, variable_name: str, results_array: np.ndarray):
        if results_array.any():
            order = self.params.get("ascending", True)
            self.ds = self.ds.sortby(self.ds[variable_name], order)
            self.record_correction(variable_name)


class SendEmailAWS(QualityHandler):
    """Send an email to the recipients using AWS services."""

    def run(self, variable_name: str, results_array: np.ndarray):
        # TODO: we will implement this later after we get the cloud
        # stuff implemented.
        pass


class FailPipeline(QualityHandler):
    """Throw an exception, halting the pipeline & indicating a critical error"""

    def run(self, variable_name: str, results_array: np.ndarray):
        if results_array.any():
            message = f"Quality Manager {self.quality_manager.name} failed for variable {variable_name}"
            raise QCError(message)
