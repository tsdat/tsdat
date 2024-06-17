import numpy as np
import xarray as xr


def decode_cf(dataset: xr.Dataset) -> xr.Dataset:
    """---------------------------------------------------------------------------------
    Wrapper around `xarray.decode_cf()` which handles additional edge cases.

    This helps ensure that the dataset is formatted and encoded correctly after it has
    been constructed or modified. Handles edge cases for units and data type encodings
    on datetime variables.

    Args:
        dataset (xr.Dataset): The dataset to decode.

    Returns:
        xr.Dataset: The decoded dataset.

    ---------------------------------------------------------------------------------"""
    # We have to make sure that time variables do not have units set as attrs, and
    # instead have units set on the encoding or else xarray will crash when trying
    # to save: https://github.com/pydata/xarray/issues/3739
    for variable in dataset.variables.values():
        if (
            np.issubdtype(variable.data.dtype, np.datetime64)  # type: ignore
            and "units" in variable.attrs
        ):
            units = variable.attrs["units"]
            del variable.attrs["units"]
            variable.encoding["units"] = units  # type: ignore

        # If the _FillValue is already encoded, remove it since it can't be overwritten per xarray
        if "_FillValue" in variable.encoding:  # type: ignore
            del variable.encoding["_FillValue"]  # type: ignore

    # Leaving the "dtype" entry in the encoding for datetime64 variables causes a crash
    # when saving the dataset. Not fixed by: https://github.com/pydata/xarray/pull/4684
    ds: xr.Dataset = xr.decode_cf(dataset)  # type: ignore
    for variable in ds.variables.values():
        if variable.data.dtype.type == np.datetime64:  # type: ignore
            if "dtype" in variable.encoding:  # type: ignore
                del variable.encoding["dtype"]  # type: ignore
    return ds
