from typing import List, Union

import numpy as np
import xarray as xr
from numpy.typing import NDArray
from pydantic import BaseModel, Extra

from .data_quality_error import DataQualityError
from ..base import QualityHandler


class FailPipeline(QualityHandler):
    """------------------------------------------------------------------------------------
    Raises a DataQualityError, halting the pipeline, if the data quality are
    sufficiently bad. This usually indicates that a manual inspection of the data is
    recommended.

    Raises:
        DataQualityError: DataQualityError

    ------------------------------------------------------------------------------------
    """

    class Parameters(BaseModel, extra=Extra.allow):
        tolerance: float = 0
        """Tolerance for the number of allowable failures as the ratio of allowable
        failures to the total number of values checked. Defaults to 0, meaning that any
        failed checks will result in a DataQualityError being raised."""

        context: str = ""
        """Additional context set by users that ends up in the traceback message."""

        display_limit: int = 5

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool_]):
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
                # TODO: IDE is giving this the following warning on the var assignment:
                #  Expected type 'list[int] | list[list[int]]', got
                #  'list[list[ndarray[Any, dtype[signedinteger | long]]]]' instead
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

    def _exceeds_tolerance(self, failures: NDArray[np.bool_]) -> bool:
        if self.parameters.tolerance == 0:
            return bool(failures.any())
        failure_ratio: float = np.average(failures)  # type: ignore
        return failure_ratio > self.parameters.tolerance
