# TODO: Implement FailPipeline
# TODO: Implement ReplaceWithFillValue
# TODO: Implement RecordQualityResults
# TODO: Implement SortDatasetByCoordinate


import numpy as np
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Literal
from numpy.typing import NDArray

from .base import QualityHandler

# def record_correction(self, variable_name: str):
#     """If a correction was made to variable data to fix invalid values
#     as detected by a quality check, this method will record the fix
#     to the appropriate variable attribute.  The correction description
#     will come from the handler params which get set in the pipeline config
#     file.

#     :param variable_name: Name
#     :type variable_name: str
#     """
#     correction = self.params.get("correction", None)
#     if correction:
#         utils.record_corrections_applied(self.ds, variable_name, correction)


# class QCParamKeys:
#     """Symbolic constants used for referencing QC-related
#     fields in the pipeline config file
#     """

#     QC_BIT = "bit"
#     ASSESSMENT = "assessment"
#     TEST_MEANING = "meaning"
#     CORRECTION = "correction"


# class DataQualityError(BaseException):
#     pass


class RecordQualityResults(QualityHandler):
    """Record the results of the quality check in an ancillary qc variable."""

    class Parameters(BaseModel, extra=Extra.forbid):
        bit: int
        assessment: Literal["bad", "indeterminate"]
        meaning: str

    parameters: Parameters

    def run(self, dataset: xr.Dataset, variable_name: str, results: NDArray[np.bool8]):
        dataset.qcfilter.add_test(
            variable_name,
            index=results,
            test_number=self.parameters.bit,
            test_meaning=self.parameters.meaning,
            test_assessment=self.parameters.assessment,
        )
        return dataset


# class RemoveFailedValues(QualityHandler):
#     """Replace all the failed values with _FillValue"""

#     def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):
#         if results_array.any():
#             fill_value = self.ds[variable_name].attrs[
#                 "_FillValue"
#             ]  # HACK: until we centralize / construct logic for this
#             keep_array = np.logical_not(results_array)

#             var_values = self.ds[variable_name].data
#             replaced_values: np.ndarray[Any, Any] = np.where(  # type: ignore
#                 keep_array, var_values, fill_value
#             )
#             self.ds[variable_name].data = replaced_values

#             self.record_correction(variable_name)


# class SortDatasetByCoordinate(QualityHandler):
#     """Sort coordinate data using xr.Dataset.sortby(). Accepts the following
#     parameters:

#     .. code-block:: yaml

#         parameters:
#           # Whether or not to sort in ascending order. Defaults to True.
#           ascending: True
#     """

#     def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):

#         if results_array.any():
#             order = self.params.get("ascending", True)
#             self.ds: xr.Dataset = self.ds.sortby(self.ds[variable_name], order)  # type: ignore
#             self.record_correction(variable_name)


# class FailPipeline(QualityHandler):
#     """Throw an exception, halting the pipeline & indicating a critical error"""

#     def run(self, variable_name: str, results_array: np.ndarray[Any, Any]):
#         if results_array.any():
#             message = f"Quality Manager {self.quality_manager.name} failed for variable {variable_name}"
#             raise DataQualityError(message)
