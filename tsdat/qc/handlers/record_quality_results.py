import logging
from typing import Any, Dict, Literal, Optional

import numpy as np
import xarray as xr
from numpy.typing import NDArray
from pydantic import BaseModel, Extra, root_validator, validator

from ..base import QualityHandler

logger = logging.getLogger(__name__)


class RecordQualityResults(QualityHandler):
    """------------------------------------------------------------------------------------
    Records the results of the quality check in an ancillary qc variable. Creates the
    ancillary qc variable if one does not already exist.

    ------------------------------------------------------------------------------------
    """

    class Parameters(BaseModel, extra=Extra.forbid):
        bit: Optional[int] = None
        """DEPRECATED

        The bit number (e.g., 1, 2, 3, ...) used to indicate if the check passed.

        The quality results are bitpacked into an integer array to preserve space. For
        example, if 'check #0' uses bit 0 and fails, and 'check #1' uses bit 1 and fails
        then the resulting value on the qc variable would be 2^(0) + 2^(1) = 3. If we
        had a third check it would be 2^(0) + 2^(1) + 2^(2) = 7."""

        assessment: Literal["bad", "indeterminate"]
        """Indicates the quality of the data if the test results indicate a failure."""

        meaning: str
        """A string that describes the test applied."""

        @root_validator(pre=True)
        def deprecate_bit_parameter(cls, values: Dict[str, Any]) -> Dict[str, Any]:
            if "bit" in values:
                logger.warning("The 'bit' argument is deprecated, please remove it.")
            return values

        @validator("assessment", pre=True)
        def to_lower(cls, assessment: Any) -> str:
            if isinstance(assessment, str):
                return assessment.lower()
            raise ValueError(
                f"assessment must be 'bad' or 'indeterminate', not {assessment}"
            )

    parameters: Parameters

    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
        failures: NDArray[np.bool_],
    ) -> xr.Dataset:
        dataset.qcfilter.add_test(
            variable_name,
            index=failures if failures.any() else None,
            test_number=self.get_next_bit_number(dataset, variable_name),
            test_meaning=self.parameters.meaning,
            test_assessment=self.parameters.assessment,
        )
        return dataset

    @staticmethod
    def get_next_bit_number(dataset: xr.Dataset, variable_name: str) -> int:
        if (qc_var := dataset.get(f"qc_{variable_name}")) is None:
            return 1
        masks = qc_var.attrs.get("flag_masks")
        if not isinstance(masks, list):
            raise ValueError(
                f"QC Variable {qc_var.name} is not standardized. Expected 'flag_masks'"
                f" attribute to be like [1, 2, ...], but found '{masks}'"
            )
        return len(masks) + 1  # type: ignore
