from typing import Literal

import numpy as np
import xarray as xr


def _create_bounds(
    coordinate: xr.DataArray,
    alignment: Literal["LEFT", "RIGHT", "CENTER"],
    width: str,
) -> xr.DataArray:
    """Creates coordinate bounds with the specified alignment and bound width."""
    coord_vals = coordinate.data
    # TODO: handle for units

    units = ""
    for i, s in enumerate(width):
        if s.isalpha():
            units = width[i:]
            width = width[:i]
    _width = float(width)

    if np.issubdtype(coordinate.dtype, np.datetime64):  # type: ignore
        coord_vals = np.array([np.datetime64(val) for val in coord_vals])
        _width = np.timedelta64(int(_width), units or "s")

    if alignment == "LEFT":
        begin = coord_vals
        end = coord_vals + _width
    elif alignment == "CENTER":
        begin = coord_vals - _width / 2
        end = coord_vals + _width / 2
    elif alignment == "RIGHT":
        begin = coord_vals - _width
        end = coord_vals

    bounds_array = np.stack((begin, end), axis=-1)  # type: ignore
    return xr.DataArray(
        bounds_array,
        dims=[coordinate.name, "bound"],
        coords={coordinate.name: coordinate},
    )
