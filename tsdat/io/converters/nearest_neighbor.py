from typing import Any, Optional

import act  # type: ignore
import xarray as xr

from ..base import DataConverter, RetrievedDataset
from ...config.dataset import DatasetConfig


class NearestNeighbor(DataConverter):
    """Maps data onto the specified coordinate grid using nearest-neighbor."""

    coord: str = "time"
    """The coordinate axis this converter should be applied on. Defaults to 'time'."""

    def convert(
        self,
        data: xr.DataArray,
        variable_name: str,
        dataset_config: DatasetConfig,
        retrieved_dataset: RetrievedDataset,
        **kwargs: Any,
    ) -> Optional[xr.DataArray]:
        # Assume that the coord index in the output matches coord index in the retrieved
        # structure.
        target_coord = retrieved_dataset.coords[self.coord]
        coord_index = dataset_config[variable_name].dims.index(self.coord)
        current_coord_name = tuple(data.coords.keys())[coord_index]

        # Create an empty DataArray with the shape we want to achieve
        new_coords = {
            k: v.data if k != current_coord_name else target_coord.data
            for k, v in data.coords.items()
        }
        tmp_data = xr.DataArray(coords=new_coords, dims=tuple(new_coords))  # type: ignore

        # Resample the data using nearest neighbor
        new_data = data.reindex_like(other=tmp_data, method="nearest")

        return new_data
