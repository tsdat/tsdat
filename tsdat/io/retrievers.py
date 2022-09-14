# TODO: Retrieval from S3; another retriever class, or parameters on the default?
# IDEA: Implement MultiDatastreamRetriever & variable finders

from datetime import datetime
import logging
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Any, Dict, List, Optional, Pattern, Tuple, cast
from ..config.dataset import DatasetConfig
from .base import Retriever, DataReader, DataConverter, Storage

__all__ = ["DefaultRetriever", "StorageRetriever"]

logger = logging.getLogger(__name__)


class RetrievedVariable(BaseModel, extra=Extra.forbid):
    name: str
    data_converters: List[DataConverter] = []


# TODO: Rename to FilteredInputConfig
class InputKeyRetrieverConfig:
    """------------------------------------------------------------------------------------
    Tracks the coords and data vars that should be retrieved for a given input key.

    ------------------------------------------------------------------------------------"""

    def __init__(self, input_key: str, retriever: "DefaultRetriever") -> None:
        self.coords: Dict[str, RetrievedVariable] = {}
        self.data_vars: Dict[str, RetrievedVariable] = {}

        def update_mapping(
            to_update: Dict[str, RetrievedVariable],
            variable_dict: Dict[str, Dict[Pattern[str], RetrievedVariable]],
        ):
            for name, retriever_dict in variable_dict.items():
                for pattern, variable_retriever in retriever_dict.items():
                    if pattern.match(input_key):
                        to_update[name] = variable_retriever
                    break

        update_mapping(self.coords, retriever.coords)  # type: ignore
        update_mapping(self.data_vars, retriever.data_vars)  # type: ignore


class DefaultRetriever(Retriever):
    """------------------------------------------------------------------------------------
    Default API for retrieving data from one or more input sources.

    Reads data from one or more inputs, renames coordinates and data variables according
    to retrieval and dataset configurations, and applies registered DataConverters to
    retrieved data.

    Args:
        readers (Dict[Pattern[str], DataReader]): A mapping of patterns to DataReaders
            that the retriever uses to determine which DataReader to use for reading any
            given input key.
        coords (Dict[str, Dict[Pattern[str], VariableRetriever]]): A dictionary mapping
            output coordinate variable names to rules for how they should be retrieved.
        data_vars (Dict[str, Dict[Pattern[str], VariableRetriever]]): A dictionary
            mapping output data variable names to rules for how they should be
            retrieved.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        merge_kwargs: Dict[str, Any] = {}
        """Keyword arguments passed to xr.merge(). This is only relevant if multiple
        input keys are provided simultaneously, or if any registered DataReader objects
        could return a dataset mapping instead of a single dataset."""

        # IDEA: option to disable retrieval of input attrs
        # retain_global_attrs: bool = True
        # retain_variable_attrs: bool = True

    parameters: Parameters = Parameters()

    readers: Dict[Pattern, DataReader]  # type: ignore
    """A dictionary of DataReaders that should be used to read data provided an input
    key."""

    coords: Dict[str, Dict[Pattern, RetrievedVariable]]  # type: ignore
    """A dictionary mapping output coordinate names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each retrieved
    coordinate variable."""

    data_vars: Dict[str, Dict[Pattern, RetrievedVariable]]  # type: ignore
    """A dictionary mapping output data variable names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each
    retrieved data variable."""

    def retrieve(
        self, input_keys: List[str], dataset_config: DatasetConfig, **kwargs: Any
    ) -> xr.Dataset:
        raw_mapping = self._get_raw_mapping(input_keys)
        dataset_mapping: Dict[str, xr.Dataset] = {}
        for key, dataset in raw_mapping.items():
            input_config = InputKeyRetrieverConfig(key, self)
            dataset = rename_variables(dataset, input_config)
            dataset = reindex_dataset_coords(dataset, dataset_config, input_config)
            dataset = run_data_converters(dataset, dataset_config, input_config)
            dataset_mapping[key] = dataset
        output_dataset = self.merge_raw_mapping(dataset_mapping)
        return output_dataset

    def _get_raw_mapping(self, input_keys: List[str]) -> Dict[str, xr.Dataset]:
        """"""
        dataset_mapping: Dict[str, xr.Dataset] = {}
        input_reader_mapping = self._match_inputs(input_keys)
        for input_key, reader in input_reader_mapping.items():  # IDEA: async
            logger.debug("Using %s to read input_key '%s'", reader, input_key)
            data = reader.read(input_key)
            if isinstance(data, xr.Dataset):
                data = {input_key: data}
            dataset_mapping.update(data)
        return dataset_mapping

    def _match_inputs(self, input_keys: List[str]) -> Dict[str, DataReader]:
        """Matches each input key to the DataReader that should be used to open it."""
        input_reader_mapping: Dict[str, DataReader] = {}
        for input_key in input_keys:
            for regex, reader in self.readers.items():  # type: ignore
                regex = cast(Pattern[str], regex)
                if regex.match(input_key):
                    input_reader_mapping[input_key] = reader
                    break
        return input_reader_mapping

    def merge_raw_mapping(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        return xr.merge(list(raw_mapping.values()), **self.parameters.merge_kwargs)  # type: ignore


def rename_variables(
    dataset: xr.Dataset, input_config: InputKeyRetrieverConfig
) -> xr.Dataset:
    """-----------------------------------------------------------------------------
    Renames variables in the retrieved dataset according to retrieval configurations.

    Args:
        raw_dataset (xr.Dataset): The raw dataset.

    Returns:
        xr.Dataset: The simplified raw dataset.

    -----------------------------------------------------------------------------"""
    to_rename: Dict[str, str] = {}  # {raw_name: output_name}
    coords_to_rename = {
        c.name: output_name for output_name, c in input_config.coords.items()
    }
    vars_to_rename = {
        v.name: output_name for output_name, v in input_config.data_vars.items()
    }
    to_rename.update(coords_to_rename)
    to_rename.update(vars_to_rename)

    for raw_name, output_name in coords_to_rename.items():
        if raw_name not in dataset:
            to_rename.pop(raw_name)
            logger.warning(
                "Coordinate variable '%s' could not be retrieved from input. Please"
                " ensure the retrieval configuration file for the '%s' coord has"
                " the 'name' property set to the exact name of the variable in the"
                " dataset returned by the input DataReader.",
                raw_name,
                output_name,
            )
    for raw_name, output_name in vars_to_rename.items():
        if raw_name not in dataset:
            to_rename.pop(raw_name)
            logger.warning(
                "Data variable '%s' could not be retrieved from input. Please"
                " ensure the retrieval configuration file for the '%s' data"
                " variable has the 'name' property set to the exact name of the"
                " variable in the dataset returned by the input DataReader.",
                raw_name,
                output_name,
            )
    return dataset.rename(to_rename)


def run_data_converters(
    dataset: xr.Dataset,
    dataset_config: DatasetConfig,
    input_config: InputKeyRetrieverConfig,
) -> xr.Dataset:
    """------------------------------------------------------------------------------------
    Runs the declared DataConverters on the dataset's coords and data_vars.

    Returns the dataset after all converters have been run.

    Args:
        dataset (xr.Dataset): The dataset to convert.
        dataset_config (DatasetConfig): The DatasetConfig

    Returns:
        xr.Dataset: The converted dataset.

    ------------------------------------------------------------------------------------"""
    for coord_name, coord_config in input_config.coords.items():
        for converter in coord_config.data_converters:
            dataset = converter.convert(dataset, dataset_config, coord_name)
    for var_name, var_config in input_config.data_vars.items():
        for converter in var_config.data_converters:
            dataset = converter.convert(dataset, dataset_config, var_name)
    return dataset


def reindex_dataset_coords(
    dataset: xr.Dataset,
    dataset_config: DatasetConfig,
    input_config: InputKeyRetrieverConfig,
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
            dataset = dataset.swap_dims({dim: expected_dim})  # type: ignore

    return dataset


def unpack_datastream_date_str(key: str) -> Tuple[str, datetime, datetime]:
    """Unpacks a datastream-date string.

    Input strings are expected to be formatted like "datastream::start_date::end_date".
    The input string is unpacked into a tuple containing the datastream, the start date,
    and the end date.

    Args:
        key (str): The datastream date string to unpack.

    Returns:
        Tuple[str, datetime, datetime]: The unpacked datastream and dates.
    """
    datastream, start_str, end_str, *_ = key.split("::")
    start = datetime.strptime(start_str, "%Y%m%d.%H%M%S")
    end = datetime.strptime(end_str, "%Y%m%d.%H%M%S")
    return datastream, start, end


class StorageRetriever(Retriever):
    """Retrieves data from the storage area."""

    # Due to the difference in how the StorageRetriever class obtains data compared with
    # other retriever classes, the inputs to the StorageRetriever also differ slightly.

    storage: Optional[Storage]

    coords: Dict[str, Dict[Pattern, RetrievedVariable]]  # type: ignore
    """A dictionary mapping output coordinate names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each retrieved
    coordinate variable."""

    data_vars: Dict[str, Dict[Pattern, RetrievedVariable]]  # type: ignore
    """A dictionary mapping output data variable names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each
    retrieved data variable."""

    def retrieve(
        self, input_keys: List[str], dataset_config: DatasetConfig, **kwargs: Any
    ) -> xr.Dataset:
        """Retrieves data from the storage area.

        Note that input_keys for the StorageRetriever follow a different format than for
        other retrievers. Here an input_key is expected to look like:
        "sgp.ingest_name.b1::20220913.000000::20220914.000000"

        This allows each input key to be broken down into three parts: the datastream
        name corresponding with a stored standardized datastream, the start date/time,
        and the end date/time. This allows the retriever to request the appropriate data
        from the storage area.

        Args:
            input_keys (List[str]): A list of input keys.
            dataset_config (DatasetConfig): The specification of the output dataset.

        Returns:
            xr.Dataset: The retrieved dataset.
        """
        retrieved_dataset = xr.Dataset()

        # Retrieve all the input data from the storage area
        input_datasets: Dict[str, xr.Dataset] = {}
        for key in input_keys:
            datastream, start, end = unpack_datastream_date_str(key)
            input_datasets[key] = self.storage.fetch_data(
                start=start, end=end, datastream=datastream
            )

        # Extract and rename the requested DataArray objects

        # Transform the requested DataArray objects

        return retrieved_dataset
