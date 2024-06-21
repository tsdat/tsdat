import xarray as xr
import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, Extra

from tsdat import QualityHandler
from mhkit.dolfyn.adv.clean import clean_fill


class CubicSplineInterp(QualityHandler):
    """----------------------------------------------------------------------------
    Interpolate over mask values in timeseries data using the specified method

    Parameters
    ----------
    variable_name : xarray.DataArray
        The dataArray to clean.
    mask : bool
        Logical tensor of elements to "nan" out and replace
    npt : int
        The number of points on either side of the bad values that
    interpolation occurs over
    method : string
        Interpolation scheme to use (linear, cubic, pchip, etc)
    max_gap : int
        Max number of consective nan's to interpolate across, must be <= npt/2

    Returns
    -------
    da : xarray.DataArray
        The dataArray with nan's filled in
    ----------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        n_points: int = 12
        method: str = "cubic"
        max_gap: int = 6

    parameters: Parameters = Parameters()

    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool_]
    ) -> xr.Dataset:
        if failures.any():
            dataset[variable_name] = clean_fill(
                dataset[variable_name],
                mask=failures,
                npt=self.parameters.n_points,
                method=self.parameters.method,
                maxgap=self.parameters.max_gap,
            )
        return dataset
