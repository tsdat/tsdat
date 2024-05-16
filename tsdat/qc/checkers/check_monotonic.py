import logging
import numpy as np
import xarray as xr
from pydantic import BaseModel, Extra, validator
from typing import Any, Dict, Literal, Optional, Union
from numpy.typing import NDArray

from .is_datetime_like import is_datetime_like
from ..base import QualityChecker

logger = logging.getLogger(__name__)


class CheckMonotonic(QualityChecker):
    """---------------------------------------------------------------------------------
    Checks if any values are not ordered strictly monotonically (i.e. values must all be
    increasing or all decreasing). The check marks values as failed if they break from
    a monotonic order.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        require_decreasing: bool = False
        require_increasing: bool = False
        dim: Optional[str] = None

        @validator("require_increasing")
        def check_monotonic_not_increasing_and_decreasing(
            cls, inc: bool, values: Dict[str, Any]
        ) -> bool:
            if inc and values["require_decreasing"]:
                raise ValueError(
                    "CheckMonotonic -> Parameters: cannot set both 'require_increasing'"
                    " and 'require_decreasing'. Please set one or both to False."
                )
            return inc

    parameters: Parameters = Parameters()

    def run(
        self,
        dataset: xr.Dataset,
        variable_name: str,
    ) -> Union[NDArray[np.bool_], None]:
        variable = dataset[variable_name]
        failures = np.full(variable.shape, False)

        if self.parameters.dim is None and len(variable.shape) == 2:
            logger.warning(
                "Variable '%s' has shape '%s'. 2D variables must provide a 'dim'"
                " parameter for the name of the dimension to check the variable for"
                " monotonicity.",
                variable_name,
                variable.shape,
            )
            return None

        if variable.values.dtype.kind in {"U", "S"}:  # type: ignore
            logger.warning(
                "Variable '%s' has dtype '%s', which is currently not supported for"
                " monotonicity checks.",
                variable_name,
                variable.values.dtype,  # type: ignore
            )
            return None

        axis = self.get_axis(variable)
        zero = np.timedelta64(0) if is_datetime_like(variable.data) else 0

        # TODO: `direction` would be better assigned as a class var on init than within a method.
        direction: Literal["increasing", "decreasing", ""] = ""

        if self.parameters.require_decreasing:
            direction = "decreasing"
        elif self.parameters.require_increasing:
            direction = "increasing"
        else:
            diff = np.diff(variable.data, axis=axis)  # type: ignore
            direction = (
                "increasing"
                if np.sum(diff > zero) >= np.sum(diff < zero)
                else "decreasing"
            )

        # Find all the values where things break, not just those flagged by diff
        # if any(failures) and not all(failures):
        if len(variable.shape) == 1:
            prev = variable.values[0]
            for i, value in enumerate(variable.values[1:]):
                success = value < prev if direction == "decreasing" else value > prev
                if success:
                    prev = value  # only update prev on success
                else:
                    failures[i + 1] = True
        else:
            # 2D diff isn't as clever with failing indexes; just report all individual
            # points that fail
            diff = np.gradient(variable.data)[axis]
            failures = diff <= zero if direction == "increasing" else diff >= zero

        return failures

    def get_axis(self, variable: xr.DataArray) -> int:
        if not (dim := self.parameters.dim):
            dim = variable.dims[0]  # type: ignore
        return variable.get_axis_num(dim)  # type: ignore
