def replace_qc_attr(output, old_name, variable_name, output_qc_name):
    """If variable name changed, must replace ancillary_variable attribute for
    act-atmos qc to work."""

    ancillary_vars = getattr(output[variable_name], "ancillary_variables", None)
    if isinstance(ancillary_vars, list):
        output[variable_name].attrs["ancillary_variables"].remove(f"qc_{old_name}")
        # Needs to be first for act-atmos qc code to work properly
        output[variable_name].attrs["ancillary_variables"].insert(
            0, f"qc_{variable_name}"
        )
    elif isinstance(ancillary_vars, str):
        output[variable_name].attrs["ancillary_variables"] = output_qc_name

    return output
