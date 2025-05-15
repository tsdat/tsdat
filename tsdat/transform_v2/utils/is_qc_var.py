import xarray as xr


def is_qc_var(data_array: xr.DataArray) -> bool:
    starts_with_qc = str(data_array.name).startswith("qc_")
    has_qc_standard_name = (
        str(data_array.attrs.get("standard_name", "")) == "quality_flag"
    )
    return starts_with_qc or has_qc_standard_name
