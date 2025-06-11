from typing import TYPE_CHECKING, Any, Optional
import xarray as xr

from ...io.base import RetrievedDataset
from .is_metric_var import is_metric_var
from .is_qc_var import is_qc_var

# Prevent any chance of runtime circular imports for typing-only imports
if TYPE_CHECKING:  # pragma: no cover
    from ...config.dataset import DatasetConfig  # pragma: no cover
    from ...io.retrievers import StorageRetriever  # pragma: no cover


def create_input_dataset(
    data: xr.DataArray,
    variable_name: str,
    coord_name: str,
    dataset_config: "DatasetConfig",
    retrieved_dataset: RetrievedDataset,
    retriever: Optional["StorageRetriever"] = None,
    input_dataset: Optional[xr.Dataset] = None,
    input_key: Optional[str] = None,
) -> Optional[xr.Dataset]:
    """
    This function prepares the input dataset for a transformation by ensuring
    that the necessary coordinate and ancillary variables are present. It retrieves
    the required variables from the retrieved dataset and input dataset, and renames
    the coordinate if necessary. If the variable cannot be transformed, it returns
    None.
    Args:
        data (xr.DataArray): The data array to be transformed.
        variable_name (str): The name of the variable being transformed.
        coord_name (str): The name of the coordinate variable.
        dataset_config (DatasetConfig): The dataset configuration object.
        retrieved_dataset (RetrievedDataset): The dataset containing retrieved variables.
        retriever (Optional[StorageRetriever]): The storage retriever object, if available.
        input_dataset (Optional[xr.Dataset]): The input dataset to pull variables from.
        input_key (Optional[str]): The key for the input dataset, if applicable.
    Returns:
        Optional[xr.Dataset]: The input dataset for the transformation, or None if
        the variable cannot be transformed.
    """

    # The variable being transformed has to have the coordinate in its dimensions.
    # If it doesn't, then we can do nothing here so we just skip.
    if coord_name not in dataset_config[variable_name].dims:
        return None

    # Some variables can't be transformed here - specifically bounds variables, qc
    # variables, and transformation metrics variables.
    # TODO: Actually transformation metrics might need to be handled here. Perhaps
    # a linear interpolation of those (no need to handle QC). It could be triggered
    # by transforming the variable that the metrics are associated with (?)
    if variable_name.endswith("_bounds") or is_qc_var(data) or is_metric_var(data):
        return None

    assert retriever is not None
    assert retriever.parameters is not None
    assert retriever.parameters.trans_params is not None
    assert input_dataset is not None
    assert input_key is not None

    # ############################################################################ #
    # Get parameters and start building up the pieces we need to perform the
    # linear transformation.

    # First, the input pieces we need: the input variable name, input coord name(s),
    # and other inputs we need as well. Note that we need this info as a mapping
    # because later on in the pipeline things are renamed, so we need to undo whatever
    retrieved_var = retriever.match_data_var(variable_name, input_key)
    retrieved_coord = retriever.match_coord(coord_name, input_key)
    assert retrieved_var is not None
    assert retrieved_coord is not None
    # TODO: Need to assert the coord is not NA (something we used to do)

    # Get the list of associated variable names (qc, bounds, metrics). These
    # correspond with entries in the output structure and the retrieved_dataset
    # object (if the user has retrieved those variables).
    output_bounds_name = f"{coord_name}_bounds"
    output_qc_name = f"qc_{variable_name}"
    output_std_name = f"{variable_name}_std"
    output_goodfrac_name = f"{variable_name}_goodfraction"

    # Like above, but for the input dataset. We preferentially pull from the
    # retrieved dataset, but if the variable exists in the input we just assume that
    # the user forgot about it, and pull it in here.
    # TODO: is this a good idea? without it, a bunch of qc variables will not be
    # retrieved, but with it there is no way to say you don't want to retrieve those.
    input_var_name = retrieved_var.name
    input_coord_name = retrieved_coord.name
    # TODO: input_var and input_coord types are str | list[str] type. want str only.
    input_bounds = f"{input_coord_name}_bounds"
    input_qc_name = f"qc_{input_var_name}"
    input_std_name = f"{input_var_name}_std"
    input_goodfrac_name = f"{input_var_name}_goodfraction"

    input_to_output_variables = {
        input_bounds: output_bounds_name,
        input_qc_name: output_qc_name,
        input_std_name: output_std_name,
        input_goodfrac_name: output_goodfrac_name,
    }

    # Build the actual input dataset for the transformation. We primarily pull from
    # the retrieved dataset, but can also pull from the input_dataset if a variable
    # is missing that should have been retrieved.
    # TODO: Maybe there should be a warning? Should the retriever be updated to add
    # QC retrievals and these other things? Pulling from input_dataset should be a
    # last-resort.
    dataset = data.to_dataset(name=variable_name)
    for input_var_name, output_var_name in input_to_output_variables.items():
        if output_var_name in retrieved_dataset.data_vars:
            ret_data = retrieved_dataset.data_vars[output_var_name]
            dataset[output_var_name] = ret_data
        elif input_var_name in input_dataset.data_vars:
            in_data = input_dataset[input_var_name]
            dataset[output_var_name] = in_data
        else:
            # The ancillary data simply does not exist, which is fine.
            continue
    try:
        # Try to ensure the coordinate is named as it is in the output dataset. This
        # is not always the case since 'data' can still have the original coordinate
        # name and dimensions. Xarray raises a ValueError if the input name isn't in
        # the dataset, which probably means that we already have the correct name.
        dataset = dataset.rename({input_coord_name: coord_name})  # type: ignore
    except ValueError:
        coord_index = dataset_config[variable_name].dims.index(coord_name)
        current_coord_name = tuple(data.coords.keys())[coord_index]
        dataset = dataset.rename({current_coord_name: coord_name})

    return dataset
