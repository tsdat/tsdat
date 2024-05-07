from typing import (
    Dict,
    NamedTuple,
)

import xarray as xr

from ...const import VarName


class RetrievedDataset(NamedTuple):
    """Maps variable names to the input DataArray the data are retrieved from."""

    coords: Dict[VarName, xr.DataArray]
    data_vars: Dict[VarName, xr.DataArray]

    # TODO: Leftover code? Remove?
    # data_vars: Dict[VarName, Tuple[xr.Dataset, xr.DataArray]]  # (input dataset, output dataset)
    # def get_output_dataset(self, variable_name: str) -> xr.DataArray

    @classmethod
    def from_xr_dataset(cls, dataset: xr.Dataset):
        coords = {str(name): data for name, data in dataset.coords.items()}
        data_vars = {str(name): data for name, data in dataset.data_vars.items()}
        return cls(coords=coords, data_vars=data_vars)
