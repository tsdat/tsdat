import xarray as xr


def is_qc_var(data_array: xr.DataArray) -> bool:
    """
    Check if the data array is a quality control (QC) variable.
    Args:
        data_array (xr.DataArray): The data array to check.
    Returns:
        bool: True if the data array is a QC variable, False otherwise.
    """
    starts_with_qc = str(data_array.name).startswith("qc_")
    has_qc_standard_name = (
        str(data_array.attrs.get("standard_name", "")) == "quality_flag"
    )
    return starts_with_qc or has_qc_standard_name
