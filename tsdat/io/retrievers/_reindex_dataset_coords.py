import logging
import xarray as xr

from .input_key_retrieval_rules import InputKeyRetrievalRules
from ...config.dataset import DatasetConfig

logger = logging.getLogger(__name__)


def _reindex_dataset_coords(
    dataset: xr.Dataset,
    dataset_config: DatasetConfig,
    input_config: InputKeyRetrievalRules,
) -> xr.Dataset:
    """-----------------------------------------------------------------------------
    Swaps dimensions and coordinates to match the structure of the DatasetConfig.

    Ensures that the retriever coordinates are set as coordinates in the dataset,
    promoting them to coordinates from data_vars as needed, and reindexes data_vars
    so they are dimensioned by the appropriate coordinates.

    This is useful in situations where the DataReader does not know which variables
    to set as coordinates in its returned xr.Dataset, so it instead creates some
    arbitrary index coordinate to dimension the data variables. This is very common
    when reading from non-heirarchal formats such as csv.

    Args:
        dataset (xr.Dataset): The dataset to reindex.
        dataset_config (DatasetConfig): The DatasetConfig.

    Returns:
        xr.Dataset: The reindexed dataset.

    -----------------------------------------------------------------------------"""
    for axis, coord_name in enumerate(input_config.coords):
        expected_dim = dataset_config[coord_name].dims[0]
        actual_dims = dataset[coord_name].dims
        if (ndims := len(actual_dims)) > 1:
            raise ValueError(
                f"Retrieved coordinate '{coord_name}' must have exactly one"
                f" dimension in the retrieved dataset, found {ndims} (dims="
                f"{actual_dims}). If '{coord_name}' is not actually a coordinate"
                " variable, please move it to the data_vars section in the"
                " retriever config file."
            )
        elif ndims == 0:
            logger.warning(
                f"Retrieved coordinate '{coord_name}' has 0 attached dimensions in"
                " the retrieved dataset (expected ndims=1). Attempting to fix this"
                f" using xr.Dataset.expand_dims(dim='{coord_name}'), which may"
                " result in unexpected behavior. Please consider writing a"
                " DataReader to handle this coordinate correctly."
            )
            dataset = dataset.expand_dims(dim=coord_name, axis=axis)
        dim = actual_dims[0] if ndims else coord_name
        if dim != expected_dim:
            # TODO: fix warning message that appears here
            dataset = dataset.swap_dims({dim: expected_dim})  # type: ignore

    return dataset
