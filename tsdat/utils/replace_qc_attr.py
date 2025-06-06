def replace_qc_attr(output, old_name, variable_name, output_qc_name):
    """If variable name changed, must replace ancillary_variable attribute for
    act-atmos qc to work."""

    if hasattr(output[variable_name], "ancillary_variables"):
        ancillary_vars = output[variable_name].attrs["ancillary_variables"]

        if isinstance(ancillary_vars, list):
            ancillary_vars.remove(f"qc_{old_name}")
            # Needs to be first for act-atmos qc code to work properly
            ancillary_vars.insert(0, f"qc_{variable_name}")
        elif isinstance(ancillary_vars, str):
            ancillary_vars = output_qc_name

        output[variable_name].attrs["ancillary_variables"] = ancillary_vars

    return output
