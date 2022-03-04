from abc import ABC, abstractmethod
from typing import Any, Dict

import numpy as np
import xarray as xr

from tsdat.config.quality import ManagerConfig
from tsdat import utils


class QCParamKeys:
    """Symbolic constants used for referencing QC-related
    fields in the pipeline config file
    """

    QC_BIT = "bit"
    ASSESSMENT = "assessment"
    TEST_MEANING = "meaning"
    CORRECTION = "correction"


class DataQualityError(BaseException):
    pass


class QualityHandler(ABC):
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

    # TODO: Add a 'name' parameter here (useful for logging / debugging)
    def __init__(
        self,
        ds: xr.Dataset,
        quality_manager: ManagerConfig,
        parameters: Dict[str, Any],
    ):
        self.ds: xr.Dataset = ds
        self.quality_manager: ManagerConfig = quality_manager
        self.params: Dict[str, Any] = parameters

    @abstractmethod
    def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):
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
        if correction:
            utils.record_corrections_applied(self.ds, variable_name, correction)


class RecordQualityResults(QualityHandler):
    """Record the results of the quality check in an ancillary qc variable."""

    def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):

        self.ds.qcfilter.add_test(
            variable_name,
            index=results_array,
            test_number=self.params.get(QCParamKeys.QC_BIT),
            test_meaning=self.params.get(QCParamKeys.TEST_MEANING),
            test_assessment=self.params.get(QCParamKeys.ASSESSMENT),
        )


class RemoveFailedValues(QualityHandler):
    """Replace all the failed values with _FillValue"""

    def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):
        if results_array.any():
            fill_value = self.ds[variable_name].attrs[
                "_FillValue"
            ]  # HACK: until we centralize / construct logic for this
            keep_array = np.logical_not(results_array)

            var_values = self.ds[variable_name].data
            replaced_values: np.ndarray[Any, Any] = np.where(  # type: ignore
                keep_array, var_values, fill_value
            )
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

    def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):

        if results_array.any():
            order = self.params.get("ascending", True)
            self.ds: xr.Dataset = self.ds.sortby(self.ds[variable_name], order)  # type: ignore
            self.record_correction(variable_name)


class FailPipeline(QualityHandler):
    """Throw an exception, halting the pipeline & indicating a critical error"""

    def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):
        if results_array.any():
            message = f"Quality Manager {self.quality_manager.name} failed for variable {variable_name}"
            raise DataQualityError(message)
