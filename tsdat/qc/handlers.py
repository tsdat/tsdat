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
    "RemoveFailedValues",
    "SortDatasetByCoordinate",
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
        tolerance: float = 0
        """Tolerance for the number of allowable failures as the ratio of allowable
        failures to the total number of values checked. Defaults to 0, meaning that any
        failed checks will result in a DataQualityError being raised."""

        context: str = ""
        """Additional context set by users that ends up in the traceback message."""

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]):
        if self._exceeds_tolerance(failures):
            raise DataQualityError(
                f"Quality results for variable {variable_name} indicate a fatal error"
                " has occurred and the pipeline should exit. Manual review of the data"
                " is recommended.\n"
                f"Extra context: '{self.parameters.context}'\n"
                f"Quality results array: {failures}"
            )

        return dataset

    def _exceeds_tolerance(self, failures: NDArray[np.bool8]) -> bool:
        failure_ratio: float = np.average(failures)  # type: ignore
        return failure_ratio > self.parameters.tolerance


class RecordQualityResults(QualityHandler):
    """------------------------------------------------------------------------------------
    Records the results of the quality check in an ancillary qc variable. Creates the
    ancillary qc variable if one does not already exist.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        bit: int = Field(ge=1, lt=32)
        """The bit number (e.g., 1, 2, 3, ...) used to indicate if the check passed.
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


class RemoveFailedValues(QualityHandler):
    """------------------------------------------------------------------------------------
    Replaces all failed values with the variable's _FillValue. If the variable does not
    have a _FillValue attribute then nan is used instead

    ------------------------------------------------------------------------------------"""

    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]
    ) -> xr.Dataset:
        if failures.any():
            fill_value = dataset[variable_name].attrs.get("_FillValue", None)
            dataset[variable_name][failures] = fill_value
        return dataset


class SortDatasetByCoordinate(QualityHandler):
    """------------------------------------------------------------------------------------
    Sorts the dataset by the failed variable, if there are any failures.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        ascending: bool = True
        """Whether to sort the dataset in ascending order. Defaults to True."""

    parameters: Parameters = Parameters()

    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]
    ) -> xr.Dataset:
        if failures.any():
            dataset = dataset.sortby(variable_name, ascending=self.parameters.ascending)  # type: ignore
        return dataset
