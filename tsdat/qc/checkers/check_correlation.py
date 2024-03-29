import xarray as xr
import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, Extra

from ..base import QualityChecker


class CheckCorrelation(QualityChecker):
    """----------------------------------------------------------------------------
    Filters out velocity data where correlation is below a
    threshold in the beam correlation data.

    Parameters
    ----------
    ds : xarray.Dataset
      The adcp dataset to clean.
    corr_threshold : numeric
      The maximum value of correlation to screen, in counts or %

    ----------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        correlation_threshold: int = 30

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:
        # Get low correlation mask
        mask = dataset["corr"].values < self.parameters.correlation_threshold

        # Combine for 1D velocity variables
        if len(dataset[variable_name].shape) == 1:
            mask = mask.sum(axis=-1).astype(bool)

        return mask
