from typing import Literal

import numpy as np
import xarray as xr
from act.qc.qcfilter import QCFilter  # type: ignore


def get_filtered_data(
    dataset: xr.Dataset, var_name: str, filter_out: Literal["Bad", "Indeterminate"]
) -> tuple[np.ndarray, np.ndarray]:
    data = QCFilter(dataset).get_masked_data(
        var_name, rm_assessments=[filter_out], return_nan_array=False
    )  # type: ignore
    return np.ma.filled(data, np.nan), data.mask
