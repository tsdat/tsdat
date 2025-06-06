import xarray as xr


def _is_std_dev_var(data_array: xr.DataArray) -> bool:
    ends_with_goodfrac = str(data_array.name).endswith("_std")
    has_metric_long_name = str(data_array.attrs.get("long_name", "")).startswith(
        "Metric std for field"
    )
    return ends_with_goodfrac or has_metric_long_name


def _is_goodfrac_var(data_array: xr.DataArray) -> bool:
    ends_with_goodfrac = str(data_array.name).endswith("_goodfraction")
    has_metric_long_name = str(data_array.attrs.get("long_name", "")).startswith(
        "Metric goodfraction for field"
    )
    return ends_with_goodfrac or has_metric_long_name


def is_metric_var(data_array: xr.DataArray) -> bool:
    return _is_std_dev_var(data_array) or _is_goodfrac_var(data_array)
