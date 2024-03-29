import xarray as xr
import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, Extra

from ..base import QualityChecker


class CheckOutliers(QualityChecker):
    """---------------------------------------------------------------------------------
    Checks data for elements greater than `n_std` standard deviations away from the mean

    Built-in implementations of quality checkers can be found in the
    [tsdat.qc.checkers](https://tsdat.readthedocs.io/en/latest/autoapi/tsdat/qc/checkers)
    module.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        n_std: int = 3

    parameters: Parameters = Parameters()

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool8]:
        n_std = self.parameters.n_std

        std_dev = dataset[variable_name].std(dim="time", ddof=1)
        mean = dataset[variable_name].std(dim="time")
        mask = dataset[variable_name] > mean + std_dev * n_std

        return mask.data
