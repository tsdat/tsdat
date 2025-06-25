import numpy as np
import xarray as xr


def add_empty_std_dev_var(dataset: xr.Dataset, input_var_name: str) -> str:
    """
    Add an empty standard deviation variable to the dataset.
    The standard deviation variable is initialized with NaN values and has the same
    shape as the input variable.
    Args:
        dataset (xr.Dataset): The dataset to which the standard deviation variable
        will be added.
        input_var_name (str): The name of the input variable for which the
        standard deviation is being created.
    Returns:
        str: The name of the newly created standard deviation variable.
    """
    std_var_name = f"{input_var_name}_std"
    dataset[std_var_name] = xr.full_like(
        dataset[input_var_name], fill_value=np.nan, dtype=np.float64
    )
    dataset[std_var_name].attrs = dict(
        long_name=f"Metric std for field {input_var_name}",
        units=dataset[input_var_name].attrs.get("units", "1"),
    )
    return std_var_name
