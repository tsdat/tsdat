def replace_qc_attr(data, old_name, variable_name, output_qc_name):
    """
    If variable name changed, must replace ancillary_variable attribute for act-atmos
    quality control to work.
    Args:
        data (xarray.DataArray or xarray.Dataset): The data object containing the variable.
        old_name (str): The original name of the variable.
        variable_name (str): The new name of the variable.
        output_qc_name (str): The new QC variable name to be used in ancillary_variables.
    Returns:
        xarray.DataArray or xarray.Dataset: The modified data object with updated ancillary_variables.
    """

    if hasattr(data, "ancillary_variables"):
        ancillary_vars = data.attrs["ancillary_variables"]

        if old_name in ancillary_vars:
            if isinstance(ancillary_vars, list):
                ancillary_vars.remove(f"qc_{old_name}")
                # Needs to be first for act-atmos qc code to work properly
                ancillary_vars.insert(0, f"qc_{variable_name}")
            elif isinstance(ancillary_vars, str):
                ancillary_vars = output_qc_name

        data.attrs["ancillary_variables"] = ancillary_vars

    return data
