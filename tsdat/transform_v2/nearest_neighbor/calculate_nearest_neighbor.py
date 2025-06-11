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
    """
    Perform a nearest neighbor reindexing on the input dataset based on the specified coordinate.
    This function creates an empty DataArray with the desired coordinate labels and uses
    the `reindex_like` method to align the input dataset with the new coordinate labels.
    Args:
        input_dataset (xr.Dataset): The input xarray Dataset to be reindexed.
        coord_name (str): The name of the coordinate variable to use for reindexing.
        coord_labels (np.ndarray): The new coordinate labels to align the dataset with.
        coord_range (Union[str, int, float]): The range tolerance for the nearest neighbor search.
            This can be a string with units (e.g., "1h" for 1 hour) or a numeric value.
    Returns:
        xr.Dataset: The reindexed xarray Dataset with the new coordinate labels.
    """

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
