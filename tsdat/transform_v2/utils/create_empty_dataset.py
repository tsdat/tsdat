import numpy as np
import xarray as xr

from .add_empty_goodfrac_var import add_empty_goodfrac_var
from .add_empty_std_dev_var import add_empty_std_dev_var
from .add_empty_transform_qc_var import add_empty_transform_qc_var
from .is_metric_var import is_metric_var
from .is_qc_var import is_qc_var


def empty_dataset_like(
    input_dataset: xr.Dataset,
    coord_name: str,
    coord_values: np.ndarray,
    coord_bounds: np.ndarray,
    add_transform_qc: bool = True,
    add_metric_vars: bool = False,
) -> xr.Dataset:
    """Creates a new xr.Dataset containing the same variables and metadata as the input
    dataset, but with new coordinate values and bounds for the provided coordinate
    variable.

    Other coordinate variables and coordinate bounds variables will remain untouched.
    All data variables that are at least partially dimensioned by the provided
    coordinate will have their metadata copied but their values will be set to NaN.
    Variables that are dimensioned solely by other coordinate variables will be copied
    directly over.

    Args:
        input_dataset (xr.Dataset): The input xarray Dataset.
        coord_name (str): The name of the coordinate variable to modify.
        coord_values (np.ndarray): The new values for the coordinate variable.
        coord_bounds (np.ndarray): The new bounds for the coordinate variable.

    Returns:
        xr.Dataset: The new xarray Dataset with modified coordinate values and bounds.
    """

    # Create a new empty dataset
    new_dataset = xr.Dataset(attrs=input_dataset.attrs)

    # Add coordinates
    for in_coord_name, in_coord_data_array in input_dataset.coords.items():
        if in_coord_name == coord_name:
            new_dataset[in_coord_name] = xr.DataArray(
                coord_values,
                dims=in_coord_data_array.dims,
                attrs=in_coord_data_array.attrs,
            )
        else:
            new_dataset[in_coord_name] = in_coord_data_array

    # Add bounds variables
    for new_coord_name in new_dataset.coords:
        bounds_name = f"{new_coord_name}_bounds"
        bounds_data_array = input_dataset.get(bounds_name, None)
        if new_coord_name == coord_name:
            new_dataset[bounds_name] = xr.DataArray(
                coord_bounds,
                dims=(new_coord_name, "bound"),
                attrs=bounds_data_array.attrs if bounds_data_array is not None else {},
            )
        elif bounds_data_array is not None:
            new_dataset[bounds_name] = bounds_data_array

    # Add data variables
    for var_name, data_array in input_dataset.data_vars.items():
        if var_name in new_dataset:  # skip bounds vars
            continue

        if coord_name in data_array.dims:
            if add_transform_qc and is_qc_var(data_array):
                continue
            if add_metric_vars and is_metric_var(data_array):
                continue

            new_shape = tuple(new_dataset.sizes[d] for d in data_array.dims)
            new_dataset[var_name] = xr.DataArray(
                np.full(new_shape, fill_value=np.nan),
                dims=data_array.dims,
                attrs=data_array.attrs,
            )
            if add_transform_qc:
                add_empty_transform_qc_var(new_dataset, var_name)

            if add_metric_vars:
                add_empty_std_dev_var(new_dataset, var_name)
                add_empty_goodfrac_var(new_dataset, var_name)
        else:
            new_dataset[var_name] = data_array

    return new_dataset
