from typing import Any, Dict

import xarray as xr

from ..base import DataReader


class NetCDFReader(DataReader):
    """---------------------------------------------------------------------------------
    Thin wrapper around xarray's `open_dataset()` function, with optional parameters
    used as keyword arguments in the function call.

    ---------------------------------------------------------------------------------"""

    parameters: Dict[str, Any] = {}

    def read(self, input_key: str) -> xr.Dataset:
        return xr.open_dataset(input_key, **self.parameters)  # type: ignore
