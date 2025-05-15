import numpy as np
import xarray as xr


def add_empty_goodfrac_var(dataset: xr.Dataset, input_var_name: str) -> str:
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
