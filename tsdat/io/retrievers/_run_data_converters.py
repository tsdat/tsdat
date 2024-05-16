import xarray as xr

from .input_key_retrieval_rules import InputKeyRetrievalRules
from ...utils import assign_data
from ...config.dataset import DatasetConfig
from ..base import (
    RetrievedDataset,
)


def _run_data_converters(
    dataset: xr.Dataset,
    dataset_config: DatasetConfig,
    input_config: InputKeyRetrievalRules,
) -> xr.Dataset:
    """------------------------------------------------------------------------------------
    Runs the declared DataConverters on the dataset's coords and data_vars.

    Returns the dataset after all converters have been run.

    Args:
        dataset (xr.Dataset): The dataset to convert.
        dataset_config (DatasetConfig): The DatasetConfig

    Returns:
        xr.Dataset: The converted dataset.

    ------------------------------------------------------------------------------------
    """
    retrieved_dataset = RetrievedDataset.from_xr_dataset(dataset)
    for coord_name, coord_config in input_config.coords.items():
        for converter in coord_config.data_converters:
            data_array = retrieved_dataset.coords[coord_name]
            data = converter.convert(
                data_array, coord_name, dataset_config, retrieved_dataset
            )
            if data is not None:
                retrieved_dataset.coords[coord_name] = data
                dataset = assign_data(dataset, data.data, coord_name)
    for var_name, var_config in input_config.data_vars.items():
        for converter in var_config.data_converters:
            data_array = retrieved_dataset.data_vars[var_name]
            data = converter.convert(
                data_array, var_name, dataset_config, retrieved_dataset
            )
            if data is not None:
                retrieved_dataset.data_vars[var_name] = data
                dataset = assign_data(dataset, data.data, var_name)
    # TODO: Convert retrieved_dataset back into the xr.Dataset and return that
    return dataset
