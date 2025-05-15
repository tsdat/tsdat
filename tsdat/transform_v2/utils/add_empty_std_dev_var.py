import numpy as np
import xarray as xr


def add_empty_std_dev_var(dataset: xr.Dataset, input_var_name: str) -> str:
    std_var_name = f"{input_var_name}_std"
    dataset[std_var_name] = xr.full_like(
        dataset[input_var_name], fill_value=np.nan, dtype=np.float64
    )
    dataset[std_var_name].attrs = dict(
        long_name=f"Metric std for field {input_var_name}",
        units=dataset[input_var_name].attrs.get("units", "1"),
    )
    return std_var_name
