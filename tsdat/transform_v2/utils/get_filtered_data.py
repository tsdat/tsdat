from typing import Literal

import numpy as np
import xarray as xr
from act.qc.qcfilter import QCFilter  # type: ignore


def get_filtered_data(
    dataset: xr.Dataset, var_name: str, filter_out: Literal["Bad", "Indeterminate"]
) -> tuple[np.ndarray, np.ndarray]:
    """
    Get filtered data from the dataset based on the specified variable name and filter type.
    Args:
        dataset (xr.Dataset): The dataset containing the data.
        var_name (str): The name of the variable to filter.
        filter_out (Literal["Bad", "Indeterminate"]): The type of filter to apply.
    Returns:
        tuple[np.ndarray, np.ndarray]: A tuple containing the filtered data as a NumPy
        array and a mask indicating the filtered values.
    """
    data = QCFilter(dataset).get_masked_data(
        var_name, rm_assessments=[filter_out], return_nan_array=False
    )  # type: ignore
    return np.ma.filled(data, np.nan), data.mask
