import numpy as np
import xarray as xr
from numpy.typing import NDArray

from ..base import QualityHandler


class RemoveFailedValues(QualityHandler):
    """------------------------------------------------------------------------------------
    Replaces all failed values with the variable's _FillValue. If the variable does not
    have a _FillValue attribute then nan is used instead

    ------------------------------------------------------------------------------------
    """

    def run(
        self, dataset: xr.Dataset, variable_name: str, failures: NDArray[np.bool_]
    ) -> xr.Dataset:
        if failures.any():
            if variable_name in dataset.dims:
                idx = np.argwhere(~failures).squeeze()
                dataset = dataset.isel({variable_name: idx})
            else:
                fill_value = dataset[variable_name].attrs.get("_FillValue", None)
                dataset[variable_name] = dataset[variable_name].where(~failures, fill_value)  # type: ignore
        return dataset
