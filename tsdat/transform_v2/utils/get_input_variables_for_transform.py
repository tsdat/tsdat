import xarray as xr

from .is_metric_var import is_metric_var
from .is_qc_var import is_qc_var


def get_input_variables_for_transform(
    input_dataset: xr.Dataset, coord_name: str
) -> dict[str, xr.DataArray]:
    """
    Get the input variables for a transform from an xarray Dataset.
    This function filters the data variables in the input dataset to include only those
    that are associated with the specified coordinate name, excluding bounds variables,
    quality control (QC) variables, and metric variables. The resulting dictionary maps
    variable names to their corresponding data arrays.
    Args:
        input_dataset (xr.Dataset): The input xarray Dataset containing data variables.
        coord_name (str): The name of the coordinate variable to filter by.
    Returns:
        dict[str, xr.DataArray]: A dictionary mapping variable names to their
        corresponding data arrays, filtered by the specified coordinate name.
    """
    input_data_variables = {
        str(var_name): data_array
        for var_name, data_array in input_dataset.data_vars.items()
        if (
            (coord_name in data_array.dims)
            and (var_name != f"{coord_name}_bounds")
            and not is_qc_var(data_array)
            and not is_metric_var(data_array)
            # and not str(var_name).startswith("__qc__")  # in-progress transform qc
            # and not str(var_name).startswith("__std__")  # in-progress metric
            # and not str(var_name).startswith("__goodfrac__")  # in-progress metric
        )
    }
    return input_data_variables
