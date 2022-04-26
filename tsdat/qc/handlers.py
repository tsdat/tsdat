# TODO: Implement ReplaceWithFillValue
# TODO: Implement RecordQualityResults
# TODO: Implement SortDatasetByCoordinate

import numpy as np
import xarray as xr
from pydantic import BaseModel, Extra, Field
from typing import Literal
from numpy.typing import NDArray
from .base import QualityHandler

__all__ = [
    "DataQualityError",
    "FailPipeline",
    "RecordQualityResults",
]

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


class DataQualityError(ValueError):
    """Raised when the quality of a variable indicates a fatal error has occurred.
    Manual review of the data in question is often recommended in this case."""


class FailPipeline(QualityHandler):
    """------------------------------------------------------------------------------------
    Raises a DataQualityError, halting the pipeline, if the data quality are
    sufficiently bad. This usually indicates that a manual inspection of the data is
    recommended.

    Raises:
        DataQualityError: DataQualityError

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.allow):
        # IDEA: add threshold of bad values (% or count) above which the error is thrown
        context: str = ""
        """Additional context set by users that ends up in the traceback message."""

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]):
        if failures.any():
            raise DataQualityError(
                f"Quality results for variable {variable_name} indicate a fatal error"
                " has occurred and the pipeline should exit. Manual review of the data"
                " is recommended.\n"
                f"Extra context: '{self.parameters.context}'\n"
                f"Quality results array: {failures}"
            )

        return dataset


class RecordQualityResults(QualityHandler):
    """------------------------------------------------------------------------------------
    Records the results of the quality check in an ancillary qc variable. Creates the
    ancillary qc variable if one does not already exist.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        bit: int = Field(ge=0, lt=32)
        """The bit number (e.g., 0, 1, 2, ...) used to indicate if the check passed.
        The quality results are bitpacked into an integer array to preserve space. For
        example, if 'check #0' uses bit 0 and fails, and 'check #1' uses bit 1 and fails
        then the resulting value on the qc variable would be 2^(0) + 2^(1) = 3. If we
        had a third check it would be 2^(0) + 2^(1) + 2^(2) = 7."""

        assessment: Literal["bad", "indeterminate"]
        """Indicates the quality of the data if the test results indicate a failure."""

        meaning: str
        """A string that describes the test applied."""

    parameters: Parameters

    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]
    ) -> xr.Dataset:
        dataset.qcfilter.add_test(
            variable_name,
            index=failures,
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
