import xarray as xr
import numpy as np
from numpy.typing import NDArray

from ..base import QualityChecker


class CheckMissing(QualityChecker):
    """---------------------------------------------------------------------------------
    Checks if any data are missing. A variable's data are considered missing if they are
    set to the variable's _FillValue (if it has a _FillValue) or NaN (NaT for datetime-
    like variables).

    ---------------------------------------------------------------------------------"""

    def run(self, dataset: xr.Dataset, variable_name: str) -> NDArray[np.bool_]:
        results: NDArray[np.bool_] = dataset[variable_name].isnull().data

        if "_FillValue" in dataset[variable_name].attrs:
            fill_value = dataset[variable_name].attrs["_FillValue"]
            results |= dataset[variable_name].data == fill_value

        elif np.issubdtype(dataset[variable_name].data.dtype, str):  # type: ignore
            fill_value = ""
            results |= dataset[variable_name].data == fill_value

        return results
