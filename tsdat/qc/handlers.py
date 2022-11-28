from typing import Any, List, Literal, Tuple, Union

import numpy as np
import xarray as xr
from numpy.typing import NDArray
from pydantic import BaseModel, Extra, Field, validator

from ..utils import record_corrections_applied
from .base import QualityHandler

__all__ = [
    "DataQualityError",
    "FailPipeline",
    "RecordQualityResults",
    "RemoveFailedValues",
    "SortDatasetByCoordinate",
]


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

        display_limit: int = 5

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]):
        if self._exceeds_tolerance(failures):  # default failure tolerance is 0%
            msg = (
                f"Quality results for variable '{variable_name}' indicate a fatal error"
                " has occurred. Manual review of the data is recommended.\n"
            )

            # Show % failed if tolerance is set
            fail_rate: float = np.average(failures)  # type: ignore
            msg += (
                f" {np.count_nonzero(failures)} / {failures.size} values failed"  # type: ignore
                f" ({100*fail_rate:.2f}%), exceeding the allowable threshold of"
                f" {100*self.parameters.tolerance}%.\n"
            )

            # Want to show the first few indexes where the test failed and also the
            # corresponding data values. Careful to not show too many, otherwise the
            # message will be bloated and hard to read. Note that np.nonzero(failures)
            # returns a hard-to-read tuple of indexes, so we modify that to be easier to
            # read and show the first self.parameters.display_limit # of errors.
            failed_where = np.nonzero(failures)  # type: ignore
            failed_values = list(dataset[variable_name].values[failed_where][: self.parameters.display_limit])  # type: ignore
            failed_indexes: Union[List[int], List[List[int]]]
            if len(failed_where) == 1:  # 1D
                failed_indexes = list(failed_where[0][: self.parameters.display_limit])
            else:
                failed_indexes = [
                    [dim_idxs[i] for dim_idxs in failed_where]
                    for i in range(
                        min(self.parameters.display_limit, len(failed_where[0]))
                    )
                ]
            msg += (
                f"The first failures occur at indexes: {failed_indexes}. The"
                f" corresponding values are: {failed_values}.\n"
            )

            raise DataQualityError(msg)
        return dataset

    def _exceeds_tolerance(self, failures: NDArray[np.bool8]) -> bool:
        if self.parameters.tolerance == 0:
            return bool(failures.any())
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

        @validator("assessment", pre=True)
        def to_lower(cls, assessment: Any) -> str:
            if isinstance(assessment, str):
                return assessment.lower()
            raise ValueError(
                f"assessment must be 'bad' or 'indeterminate', not {assessment}"
            )

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
            dataset[variable_name] = dataset[variable_name].where(~failures, fill_value)  # type: ignore
        return dataset


class SortDatasetByCoordinate(QualityHandler):
    """------------------------------------------------------------------------------------
    Sorts the dataset by the failed variable, if there are any failures.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        ascending: bool = True
        """Whether to sort the dataset in ascending order. Defaults to True."""

        correction: str = "Coordinate data was sorted in order to ensure monotonicity."

    parameters: Parameters = Parameters()

    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool8]
    ) -> xr.Dataset:
        if failures.any():
            dataset = dataset.sortby(variable_name, ascending=self.parameters.ascending)  # type: ignore
            record_corrections_applied(
                dataset, variable_name, self.parameters.correction
            )
        return dataset
