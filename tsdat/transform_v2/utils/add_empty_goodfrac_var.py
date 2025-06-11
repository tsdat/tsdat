import numpy as np
import xarray as xr


def add_empty_goodfrac_var(dataset: xr.Dataset, input_var_name: str) -> str:
    """
    Add an empty goodfraction variable to the dataset.
    The goodfraction variable is initialized with NaN values and has the same
    shape as the input variable.
    Args:
        dataset (xr.Dataset): The dataset to which the goodfraction variable
                              will be added.
        input_var_name (str): The name of the input variable for which the
                              goodfraction is being created.
    Returns:
        str: The name of the newly created goodfraction variable.
    """
    goodfrac_var_name = f"{input_var_name}_goodfraction"
    dataset[goodfrac_var_name] = xr.full_like(
        dataset[input_var_name],
        fill_value=np.nan,
        dtype=np.float64,
    )
    dataset[goodfrac_var_name].attrs = dict(
        long_name=f"Metric goodfraction for field {input_var_name}", units="1"
    )
    return goodfrac_var_name
