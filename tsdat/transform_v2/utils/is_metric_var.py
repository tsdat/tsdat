import xarray as xr


def _is_std_dev_var(data_array: xr.DataArray) -> bool:
    """
    Check if the data array is a standard deviation variable.
    Args:
        data_array (xr.DataArray): The data array to check.
    Returns:
        bool: True if the data array is a standard deviation variable, False otherwise.
    """
    ends_with_goodfrac = str(data_array.name).endswith("_std")
    has_metric_long_name = str(data_array.attrs.get("long_name", "")).startswith(
        "Metric std for field"
    )
    return ends_with_goodfrac or has_metric_long_name


def _is_goodfrac_var(data_array: xr.DataArray) -> bool:
    """
    Check if the data array is a good fraction variable.
    Args:
        data_array (xr.DataArray): The data array to check.
    Returns:
        bool: True if the data array is a good fraction variable, False otherwise.
    """
    ends_with_goodfrac = str(data_array.name).endswith("_goodfraction")
    has_metric_long_name = str(data_array.attrs.get("long_name", "")).startswith(
        "Metric goodfraction for field"
    )
    return ends_with_goodfrac or has_metric_long_name


def is_metric_var(data_array: xr.DataArray) -> bool:
    """
    Check if the data array is a metric variable.
    Args:
        data_array (xr.DataArray): The data array to check.
    Returns:
        bool: True if the data array is a metric variable, False otherwise.
    """
    return _is_std_dev_var(data_array) or _is_goodfrac_var(data_array)
