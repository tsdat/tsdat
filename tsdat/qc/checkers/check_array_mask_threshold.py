import xarray as xr
from numpy.typing import NDArray
from pydantic import BaseModel, Extra
from enum import Enum

from ..base import QualityChecker


class Comparitor(str, Enum):
    less_than = "<"
    less_than_eq = "<="
    greater_than = ">"
    greater_than_eq = ">="
    equal = "=="
    not_equal = "!"


class CheckArrayMaskThreshold(QualityChecker):
    """----------------------------------------------------------------------------
    Filters out velocity data where correlation is below a
    threshold in the beam correlation data.

    Parameters
    ----------
    correlation_threshold : numeric
      The maximum value of correlation to screen, in counts or %
    ----------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        correlation_threshold: int = 30
        comparitor: Comparitor = Comparitor.less_than

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[bool]:
        # Get mask based on specified comparitor
        match self.parameters.comparitor:
            case Comparitor.less_than:
                mask = (
                    dataset[variable_name].values
                    < self.parameters.correlation_threshold
                )
            case Comparitor.less_than_eq:
                mask = (
                    dataset[variable_name].values
                    <= self.parameters.correlation_threshold
                )
            case Comparitor.greater_than:
                mask = (
                    dataset[variable_name].values
                    > self.parameters.correlation_threshold
                )
            case Comparitor.greater_than_eq:
                mask = (
                    dataset[variable_name].values
                    >= self.parameters.correlation_threshold
                )
            case Comparitor.equal:
                mask = (
                    dataset[variable_name].values
                    == self.parameters.correlation_threshold
                )
            case Comparitor.not_equal:
                mask = (
                    dataset[variable_name].values
                    != self.parameters.correlation_threshold
                )
            case _:
                raise ValueError(
                    f"`comparitor` parameter of {self.parameters.comparitor} is invalid."
                    f" Please specify one of `<`, `<=`, `>`, `>=`, `==`, or `!=`."
                )

        # Combine for 1D velocity variables
        if len(dataset[variable_name].shape) == 1:
            mask = mask.sum(axis=-1).astype(bool)

        return mask
