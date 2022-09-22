from datetime import datetime
import logging
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Any, Dict, List, Optional, Pattern, Tuple, cast

from ..utils import assign_data
from ..config.dataset import DatasetConfig
from .base import (
    DataReader,
    InputKey,
    RetrievalRuleSelections,
    RetrievedDataset,
    RetrievedVariable,
    Retriever,
    Storage,
    VarName,
)

# TODO: Note that the DefaultRetriever applies DataConverters / transformations on
# variables from all input datasets, while the new version only applies these to
# variables that are actually retrieved. This leads to a different way of applying
# data converters. Maybe they should both use the StorageRetriever approach.

__all__ = ["DefaultRetriever", "StorageRetriever"]

logger = logging.getLogger(__name__)


class InputKeyRetrievalRules:
    """Gathers variable retrieval rules for the given input key."""

    def __init__(
        self,
        input_key: InputKey,
        coord_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
        data_var_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
    ):
        self.coords: Dict[VarName, RetrievedVariable] = {}
        self.data_vars: Dict[VarName, RetrievedVariable] = {}

        for name, retriever_dict in coord_rules.items():
            for pattern, variable_retriever in retriever_dict.items():
                if pattern.match(input_key):
                    self.coords[name] = variable_retriever
                break

        for name, retriever_dict in data_var_rules.items():
            for pattern, variable_retriever in retriever_dict.items():
                if pattern.match(input_key):
                    self.data_vars[name] = variable_retriever
                break


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

    def retrieve(
        self, input_keys: List[str], dataset_config: DatasetConfig, **kwargs: Any
    ) -> xr.Dataset:
        raw_mapping = self._get_raw_mapping(input_keys)
        dataset_mapping: Dict[str, xr.Dataset] = {}
        for key, dataset in raw_mapping.items():
            input_config = InputKeyRetrievalRules(
                input_key=key,
                coord_rules=self.coords,  # type: ignore
                data_var_rules=self.data_vars,  # type: ignore
            )
            dataset = _rename_variables(dataset, input_config)
            dataset = _reindex_dataset_coords(dataset, dataset_config, input_config)
            dataset = _run_data_converters(dataset, dataset_config, input_config)
            dataset_mapping[key] = dataset
        output_dataset = self._merge_raw_mapping(dataset_mapping)
        return output_dataset

    def _get_raw_mapping(self, input_keys: List[str]) -> Dict[str, xr.Dataset]:
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
        input_reader_mapping: Dict[str, DataReader] = {}
        for input_key in input_keys:
            for regex, reader in self.readers.items():  # type: ignore
                regex = cast(Pattern[str], regex)
                if regex.match(input_key):
                    input_reader_mapping[input_key] = reader
                    break
        return input_reader_mapping

    def _merge_raw_mapping(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        return xr.merge(list(raw_mapping.values()), **self.parameters.merge_kwargs)  # type: ignore


def _rename_variables(
    dataset: xr.Dataset,
    input_config: InputKeyRetrievalRules,
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
            input_config.coords.pop(raw_name)
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
            input_config.data_vars.pop(raw_name)
            logger.warning(
                "Data variable '%s' could not be retrieved from input. Please"
                " ensure the retrieval configuration file for the '%s' data"
                " variable has the 'name' property set to the exact name of the"
                " variable in the dataset returned by the input DataReader.",
                raw_name,
                output_name,
            )
    return dataset.rename(to_rename)


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

    ------------------------------------------------------------------------------------"""
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


def perform_data_retrieval(
    input_data: Dict[InputKey, xr.Dataset],
    coord_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
    data_var_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
) -> Tuple[RetrievedDataset, RetrievalRuleSelections]:
    # Rule selections
    selected_coord_rules: Dict[VarName, RetrievedVariable] = {}
    selected_data_var_rules: Dict[VarName, RetrievedVariable] = {}

    # Retrieved dataset
    coord_data: Dict[VarName, xr.DataArray] = {}
    data_var_data: Dict[VarName, xr.DataArray] = {}

    # Retrieve coordinates
    for name, retriever_dict in coord_rules.items():
        for pattern, variable_retriever in retriever_dict.items():
            if name in selected_coord_rules:  # already matched
                break
            for input_key, dataset in input_data.items():
                if pattern.match(input_key):
                    if variable_retriever.name in dataset.variables:
                        logger.info(
                            "Coordinate '%s' retrieved from '%s': '%s'",
                            name,
                            input_key,
                            variable_retriever.name,
                        )
                        selected_coord_rules[name] = variable_retriever
                        coord_data[name] = dataset[variable_retriever.name]
                        break
                    else:
                        logger.warning(
                            "Input key matched regex pattern but no matching variable"
                            " could be found in the input dataset:\n"
                            "\tCoordinate: %s\n"
                            "\tInput Variable: %s\n"
                            "\tPattern: %s\n"
                            "\tInput Key: %s\n",
                            name,
                            variable_retriever.name,
                            pattern.pattern,
                            input_key,
                        )
        if name not in selected_coord_rules:
            logger.warning("Could not retrieve coordinate '%s'.", name)

    # Retrieve data variables
    for name, retriever_dict in data_var_rules.items():
        for pattern, variable_retriever in retriever_dict.items():
            if name in selected_data_var_rules:  # already matched
                break
            for input_key, dataset in input_data.items():
                if pattern.match(input_key):
                    if variable_retriever.name in dataset.variables:
                        logger.info(
                            "Variable '%s' retrieved from '%s': '%s'",
                            name,
                            input_key,
                            variable_retriever.name,
                        )
                        selected_data_var_rules[name] = variable_retriever
                        data_var_data[name] = dataset[variable_retriever.name]
                        break
                    else:
                        logger.warning(
                            "Input key matched regex pattern but no matching variable"
                            " could be found in the input dataset:\n"
                            "\tVariable: %s\n"
                            "\tInput Variable: %s\n"
                            "\tPattern: %s\n"
                            "\tInput Key: %s\n",
                            name,
                            variable_retriever.name,
                            pattern.pattern,
                            input_key,
                        )
        if name not in selected_data_var_rules:
            logger.warning("Could not retrieve variable '%s'.", name)

    return (
        RetrievedDataset(coords=coord_data, data_vars=data_var_data),
        RetrievalRuleSelections(
            coords=selected_coord_rules, data_vars=selected_data_var_rules
        ),
    )


class StorageRetriever(Retriever):
    """Retriever API for pulling input data from the storage area."""

    def retrieve(
        self,
        input_keys: List[str],
        dataset_config: DatasetConfig,
        storage: Optional[Storage] = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Retrieves input data from the storage area.

        Note that each input_key is expected to be formatted according to the following
        format:

        "datastream::start-date::end-date",

        e.g., "sgp.myingest.b1::20220913.000000::20220914.000000"

        This format allows the retriever to pull datastream data from the Storage API
        for the desired dates for each desired input source.

        Args:
            input_keys (List[str]): A list of specially-formatted input keys.
            dataset_config (DatasetConfig): The output dataset configuration.
            storage (Storage): Instance of a Storage class used to fetch saved data.

        Returns:
            xr.Dataset: The retrieved dataset

        ------------------------------------------------------------------------------------"""
        assert storage is not None, "Missing required 'storage' parameter."

        # Use the Storage API to fetch input data
        input_data: Dict[InputKey, xr.Dataset] = {}
        for key in input_keys:
            datastream, start, end = unpack_datastream_date_str(key)
            # TODO: pad start & end according to parameters
            retrieved_dataset = storage.fetch_data(
                start=start, end=end, datastream=datastream
            )
            input_data[key] = retrieved_dataset

        # Perform coord/variable retrieval
        retrieved_data, retrieval_selections = perform_data_retrieval(
            input_data=input_data,
            coord_rules=self.coords,  # type: ignore
            data_var_rules=self.data_vars,  # type: ignore
        )

        # Ensure selected coords are indexed by themselves
        for name, coord_data in retrieved_data.coords.items():
            new_coord = xr.DataArray(
                data=coord_data.data,
                coords={name: coord_data.data},
                dims=(name,),
                attrs=coord_data.attrs,
                name=name,
            )
            retrieved_data.coords[name] = new_coord
        # Q: Do data_vars need to be renamed or reindexed before data converters run?

        # Run data converters on coordinates, then on data variables
        for name, coord_def in retrieval_selections.coords.items():
            for converter in coord_def.data_converters:
                coord_data = retrieved_data.coords[name]
                data = converter.convert(
                    data=coord_data,
                    variable_name=name,
                    dataset_config=dataset_config,
                    retrieved_dataset=retrieved_data,
                )
                if data is not None:
                    retrieved_data.coords[name] = data
            # TODO: Add one more converter to make sure data type is correct

        for name, var_def in retrieval_selections.data_vars.items():
            # Q: DataArray.coords match RetrievedDataset.coords structure?
            for converter in var_def.data_converters:
                var_data = retrieved_data.data_vars[name]
                data = converter.convert(
                    data=var_data,
                    variable_name=name,
                    dataset_config=dataset_config,
                    retrieved_dataset=retrieved_data,
                )
                if data is not None:
                    retrieved_data.data_vars[name] = data
            # TODO: Add one more converter to make sure data type is correct

        # Construct the retrieved dataset structure
        retrieved_dataset = xr.Dataset(
            coords=retrieved_data.coords, data_vars=retrieved_data.data_vars
        )
        for var_name, var_data in retrieved_dataset.data_vars.items():
            # Ensure that the encoding is correct
            var_data.encoding["dtype"] = dataset_config[var_name].dtype
        return retrieved_dataset
