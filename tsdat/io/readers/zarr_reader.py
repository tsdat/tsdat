from typing import Any, Dict

import xarray as xr
from pydantic import BaseModel, Extra

from ..base import DataReader


class ZarrReader(DataReader):
    """---------------------------------------------------------------------------------
    Uses xarray's Zarr capabilities to read a Zarr archive and extract its contents into
    an xarray Dataset object.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        open_zarr_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: str) -> xr.Dataset:
        return xr.open_zarr(input_key, **self.parameters.open_zarr_kwargs)  # type: ignore
