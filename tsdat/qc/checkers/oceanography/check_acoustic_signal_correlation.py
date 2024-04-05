import xarray as xr
from numpy.typing import NDArray
from pydantic import BaseModel, Extra

from ...base import QualityChecker


class CheckAcousticSignalCorrelation(QualityChecker):
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
        below_above: str = 'below'
        eq: bool = False

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[bool]:
        # Get low correlation mask
        if self.parameters.below_above == 'below':
            mask = dataset[variable_name].values <= self.parameters.correlation_threshold \
                if self.parameters.eq \
                else dataset[variable_name].values < self.parameters.correlation_threshold
        elif self.parameters.below_above == 'above':
            mask = dataset[variable_name].values >= self.parameters.correlation_threshold \
                if self.parameters.eq \
                else dataset[variable_name].values > self.parameters.correlation_threshold
        else:
            raise ValueError(f"`below_above` parameter should only be specified as"
                             f" `below` or `above`, not {self.parameters.below_above}.")

        # Combine for 1D velocity variables
        if len(dataset[variable_name].shape) == 1:
            mask = mask.sum(axis=-1).astype(bool)

        return mask
