from typing import Union
import numpy as np
import xarray as xr

from ._get_tolerance import get_tolerance


def nearest_neighbor(
    input_dataset: xr.Dataset,
    coord_name: str,
    coord_labels: np.ndarray,
    coord_range: Union[str, int, float],
) -> xr.Dataset:

    # Create an empty DataArray with the shape we want to achieve
    new_coords = {coord_name: coord_labels}
    tmp_data = xr.DataArray(coords=new_coords, dims=tuple(new_coords))
    # Get index tolerance from coordinate
    tolerance = get_tolerance(coord_labels, coord_range)

    # Do nearest neighbor algorithm
    output_dataset = input_dataset.reindex_like(
        other=tmp_data,
        method="nearest",
        tolerance=tolerance,  # type: ignore
    )

    return output_dataset
