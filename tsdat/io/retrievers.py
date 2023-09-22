from collections import defaultdict
from datetime import datetime, timedelta
import logging
import re
import shlex
import pandas as pd
import xarray as xr
from pydantic import BaseModel, Extra, Field, validator
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Pattern,
    Tuple,
    cast,
)

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

OutputVarName = str


# TODO: Note that the DefaultRetriever applies DataConverters / transformations on
# variables from all input datasets, while the new version only applies these to
# variables that are actually retrieved. This leads to a different way of applying
# data converters. Maybe they should both use the StorageRetriever approach.

__all__ = ["DefaultRetriever", "StorageRetriever", "StorageRetrieverInput"]

logger = logging.getLogger(__name__)


class InputKeyRetrievalRules:
    """Gathers variable retrieval rules for the given input key."""

    def __init__(
        self,
        input_key: InputKey,
        coord_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
        data_var_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
    ):
        self.input_key = input_key
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


class StorageRetrieverInput:
    """Returns an object representation of an input storage key.

    Input storage keys should be formatted like:

    ```python
    "--datastream sgp.met.b0 --start 20230801 --end 20230901"
    "--datastream sgp.met.b0 --start 20230801 --end 20230901 --location_id sgp --data_level b0"
    ```
    """

    def __init__(self, input_key: str):
        kwargs: Dict[str, str] = {}

        if len(input_key.split("::")) == 3:
            logger.warning(
                "Using old Storage input key format (datastream::start::end)."
            )
            datastream, _start, _end = input_key.split("::")
            kwargs["datastream"] = datastream
            kwargs["start"] = _start
            kwargs["end"] = _end
        else:
            args = shlex.split(input_key)
            key = ""
            for arg in args:
                if arg.startswith("-"):
                    key = arg.lstrip("-")
                    kwargs[key] = ""
                elif key in kwargs:
                    kwargs[key] = arg
                    key = ""
                else:
                    raise ValueError(
                        "Bad storage retriever input key. Expected format like"
                        f" '--key1 value1 --key2 value2 ...', got '{input_key}'."
                    )

        self.input_key = input_key
        self.datastream = kwargs.pop("datastream")
        self._start = kwargs.pop("start")
        self._end = kwargs.pop("end")

        start_format = "%Y%m%d.%H%M%S" if "." in self._start else "%Y%m%d"
        end_format = "%Y%m%d.%H%M%S" if "." in self._end else "%Y%m%d"
        self.start = datetime.strptime(self._start, start_format)
        self.end = datetime.strptime(self._end, end_format)

        self.kwargs = kwargs

    def __repr__(self) -> str:
        args = f"datastream={self.datastream}, start={self._start}, end={self._end}"
        kwargs = ", ".join([f"{k}={v}" for k, v in self.kwargs.items()])
        return f"StorageRetrieverInput({args}, {kwargs})"


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

    ------------------------------------------------------------------------------------
    """

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

    def rename_vars(input_data: Dict[VarName, RetrievedVariable]):
        # Run through list of retreiver variables and organizes them into
        # a simple {input: output} dictionary
        data_to_rename: Dict[str, str] = {}
        for output_name, d in input_data.items():
            if isinstance(d.name, list):
                for n in d.name:
                    data_to_rename[n] = output_name
            else:
                data_to_rename[d.name] = output_name  # type: ignore
        return data_to_rename

    def drop_var_input_config(
        input_data: Dict[VarName, RetrievedVariable], output_name: str
    ):
        # Drop output_name from input_config.coords or input_config.data_vars
        n = input_data[output_name].name  # type: ignore
        if isinstance(n, list):
            n.remove(raw_name)  # type: ignore
            if len(n) == 1:
                input_data[output_name].name = n[0]
        else:
            input_data.pop(output_name)

    to_rename: Dict[str, str] = {}  # {raw_name: output_name}
    coords_to_rename = rename_vars(input_config.coords)
    vars_to_rename = rename_vars(input_config.data_vars)

    to_rename.update(coords_to_rename)
    to_rename.update(vars_to_rename)

    # Check for multiple raw names here
    for raw_name, output_name in coords_to_rename.items():
        if raw_name not in dataset:
            to_rename.pop(raw_name)
            drop_var_input_config(input_config.coords, output_name)
            logger.warning(
                "Coordinate variable '%s' could not be retrieved from input. Please"
                " ensure the retrieval configuration file for the '%s' coord has"
                " the 'name' property set to the exact name of the variable in the"
                " dataset returned by the input DataReader.",
                raw_name,
                output_name,
            )
        # Don't rename coordinate if name hasn't changed
        elif raw_name == output_name:
            to_rename.pop(raw_name)

    for raw_name, output_name in vars_to_rename.items():
        if raw_name not in dataset:
            to_rename.pop(raw_name)
            drop_var_input_config(input_config.data_vars, output_name)
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


def perform_data_retrieval(
    input_data: Dict[InputKey, xr.Dataset],
    coord_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
    data_var_rules: Dict[VarName, Dict[Pattern[Any], RetrievedVariable]],
) -> Tuple[RetrievedDataset, RetrievalRuleSelections]:
    # TODO: Also retrieve QC and Bounds variables -- possibly in ancillary structure?

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
                    logger.info(
                        "Coordinate '%s' retrieved from '%s': '%s'",
                        name,
                        input_key,
                        variable_retriever.name,
                    )
                    coord_data[name] = dataset.get(
                        variable_retriever.name, xr.DataArray([])
                    )
                    if not coord_data[name].equals(xr.DataArray([])):
                        variable_retriever.source = input_key
                    selected_coord_rules[name] = variable_retriever
                    break
        if name not in selected_coord_rules:
            logger.warning("Could not retrieve coordinate '%s'.", name)

    # Retrieve data variables
    for name, retriever_dict in data_var_rules.items():
        for pattern, variable_retriever in retriever_dict.items():
            if name in selected_data_var_rules:  # already matched
                break
            for input_key, dataset in input_data.items():
                if pattern.match(input_key):
                    logger.info(
                        "Variable '%s' retrieved from '%s': '%s'",
                        name,
                        input_key,
                        variable_retriever.name,
                    )
                    data_var_data[name] = dataset.get(
                        variable_retriever.name, xr.DataArray([])
                    )
                    if data_var_data[name].equals(xr.DataArray([])):
                        logger.warning(
                            "Input key matched regex pattern but no matching variable"
                            " could be found in the input dataset. A value of"
                            " xr.DataArray([]) will be used instead.\n"
                            "\tVariable: %s\n"
                            "\tInput Variable: %s\n"
                            "\tPattern: %s\n"
                            "\tInput Key: %s\n",
                            name,
                            variable_retriever.name,
                            pattern.pattern,
                            input_key,
                        )
                    variable_retriever.source = input_key
                    selected_data_var_rules[name] = variable_retriever
                    break
        if name not in selected_data_var_rules:
            logger.warning("Could not retrieve variable '%s'.", name)

    return (
        RetrievedDataset(coords=coord_data, data_vars=data_var_data),
        RetrievalRuleSelections(
            coords=selected_coord_rules, data_vars=selected_data_var_rules
        ),
    )

    # TODO: set default dim_range for time dim (ARM uses 1 day)


class GlobalARMTransformParams(BaseModel):
    # TODO: Make this optional
    alignment: Dict[Pattern, Dict[str, Literal["LEFT", "RIGHT", "CENTER"]]]  # type: ignore
    dim_range: Dict[Pattern, Dict[str, str]] = Field(..., alias="range")  # type: ignore
    width: Dict[Pattern, Dict[str, str]]  # type: ignore

    @validator("alignment", "dim_range", "width", pre=True)
    def default_pattern(cls, d: Dict[Any, Any]) -> Dict[Pattern[str], Dict[str, str]]:
        if not d:
            return {}
        pattern_dict: Dict[Pattern[str], Dict[str, str]] = defaultdict(dict)
        for k, v in d.items():
            if isinstance(v, dict):
                pattern_dict[re.compile(k)] = v
            else:
                pattern_dict[re.compile(r".*")][k] = v
        return pattern_dict

    def select_parameters(self, input_key: str) -> Dict[str, Dict[str, Any]]:
        selected_params: Dict[str, Dict[str, Any]] = {
            "alignment": {},
            "range": {},
            "width": {},
        }
        for pattern, params in self.alignment.items():
            if pattern.match(input_key) is not None:
                selected_params["alignment"] = params.copy()
                break

        for pattern, params in self.dim_range.items():
            if pattern.match(input_key) is not None:
                selected_params["range"] = params.copy()
                break

        for pattern, params in self.width.items():
            if pattern.match(input_key) is not None:
                selected_params["width"] = params.copy()
                break

        return selected_params


class StorageRetriever(Retriever):
    """Retriever API for pulling input data from the storage area."""

    class TransParameters(BaseModel):
        trans_params: Optional[GlobalARMTransformParams] = Field(
            default=None, alias="transformation_parameters"
        )

    parameters: Optional[TransParameters] = None

    def retrieve(
        self,
        input_keys: List[str],
        dataset_config: DatasetConfig,
        storage: Optional[Storage] = None,
        input_data_hook: Optional[
            Callable[[Dict[str, xr.Dataset]], Dict[str, xr.Dataset]]
        ] = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Retrieves input data from the storage area.

        Note that each input_key is expected to be formatted according to the following
        format:

        ```python
        "--key1 value1 --key2 value2",
        ```

        e.g.,

        ```python
        "--datastream sgp.met.b0 --start 20230801 --end 20230901"
        "--datastream sgp.met.b0 --start 20230801 --end 20230901 --location_id sgp --data_level b0"
        ```

        This format allows the retriever to pull datastream data from the Storage API
        for the desired dates for each desired input source.

        Args:
            input_keys (List[str]): A list of input keys formatted as described above.
            dataset_config (DatasetConfig): The output dataset configuration.
            storage (Storage): Instance of a Storage class used to fetch saved data.

        Returns:
            xr.Dataset: The retrieved dataset

        ------------------------------------------------------------------------------------
        """
        assert storage is not None, "Missing required 'storage' parameter."

        storage_input_keys = [StorageRetrieverInput(key) for key in input_keys]

        input_data = self.__fetch_inputs(storage_input_keys, storage)

        if input_data_hook is not None:
            modded_input_data = input_data_hook(input_data)
            if modded_input_data is not None:
                input_data = modded_input_data

        # Perform coord/variable retrieval
        retrieved_data, retrieval_selections = perform_data_retrieval(
            input_data=input_data,
            coord_rules=self.coords,  # type: ignore
            data_var_rules=self.data_vars,  # type: ignore
        )

        # Ensure selected coords are indexed by themselves
        for name, coord_data in retrieved_data.coords.items():
            if coord_data.equals(xr.DataArray([])):
                continue
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
                    time_span=(storage_input_keys[0].start, storage_input_keys[0].end),
                    input_dataset=input_data.get(coord_def.source),
                    retriever=self,
                    input_key=coord_def.source,
                )
                if data is not None:
                    retrieved_data.coords[name] = data

        for name, var_def in retrieval_selections.data_vars.items():
            for converter in var_def.data_converters:
                var_data = retrieved_data.data_vars[name]
                data = converter.convert(
                    data=var_data,
                    variable_name=name,
                    dataset_config=dataset_config,
                    retrieved_dataset=retrieved_data,
                    retriever=self,
                    input_dataset=input_data.get(var_def.source),
                    input_key=var_def.source,
                )
                if data is not None:
                    retrieved_data.data_vars[name] = data

        # Construct the retrieved dataset structure
        # TODO: validate dimension alignment
        retrieved_dataset = xr.Dataset(
            coords=retrieved_data.coords,
            data_vars=retrieved_data.data_vars,
        )

        # Fix the dtype encoding
        for var_name, var_data in retrieved_dataset.data_vars.items():
            output_var_cfg = dataset_config.data_vars.get(var_name)
            if output_var_cfg is not None:
                dtype = output_var_cfg.dtype
                retrieved_dataset[var_name] = var_data.astype(dtype)
                var_data.encoding["dtype"] = dtype

        return retrieved_dataset

    def _get_retrieval_padding(self, input_key: str) -> timedelta:
        if self.parameters is None or self.parameters.trans_params is None:
            return timedelta()
        params = self.parameters.trans_params.select_parameters(input_key)
        return max(
            pd.Timedelta(params["range"].get("time", "0s")),
            pd.Timedelta(params["width"].get("time", "0s")),
        )

    def __fetch_inputs(
        self, input_keys: List[StorageRetrieverInput], storage: Storage
    ) -> Dict[InputKey, xr.Dataset]:
        input_data: Dict[InputKey, xr.Dataset] = {}
        for key in input_keys:
            padding = self._get_retrieval_padding(key.input_key)
            retrieved_dataset = storage.fetch_data(
                start=key.start - padding,
                end=key.end + padding,
                datastream=key.datastream,
                metadata_kwargs=key.kwargs,
            )
            input_data[key.input_key] = retrieved_dataset
        return input_data


# class ImprovedDefaultRetriever(Retriever):

#     # TODO: Need some way to also retrieve ancillary variables (QC and Bounds)

#     def get_input_datasets(
#         self, input_keys: List[str], **kwargs: Any
#     ) -> Dict[InputKey, xr.Dataset]:
#         """Reads in the input data and returns a map of input_key: xr.Dataset."""
#         input_datasets: Dict[InputKey, xr.Dataset] = {}
#         assert self.readers is not None  # type: ignore
#         for input_key in input_keys:
#             for pattern, reader in self.readers.items():  # type: ignore
#                 if pattern.match(input_key):  # type: ignore
#                     input_datasets[input_key] = reader.read(input_key, **kwargs)
#                     break
#         return input_datasets

#     def retrieve_variable_arrays(
#         self, input_datasets: Dict[InputKey, xr.Dataset], **kwargs: Any
#     ) -> Dict[OutputVarName, Tuple[Pattern[str], List[xr.DataArray]]]:
#         """Uses retrieval config parameters to extract the variable data arrays that can
#         be retrieved. For each variable, only the first matching pattern is considered.
#         """
#         ...

#     def select_retrieved_variables(
#         self,
#         retrieved_variable_arrays: Dict[
#             OutputVarName, Tuple[Pattern[str], List[xr.DataArray]]
#         ],
#         method: Literal["merge", "first"],  # if merge then combine, if first then idx 0
#         **kwargs: Any,
#     ) -> Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]]:
#         ...

#     def convert_data(
#         self,
#         retrieved_variables: Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]],
#         input_datasets: Dict[InputKey, xr.Dataset],  # Needed to get bounds/qc
#         **kwargs: Any,
#     ) -> Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]]:
#         ...

#     def create_output_dataset(
#         self,
#         converted_data: Dict[OutputVarName, Tuple[Pattern[str], xr.DataArray]],
#         # More needed here
#     ) -> xr.Dataset:
#         ...

#     def retrieve(
#         self,
#         input_keys: List[str],
#         dataset_config: DatasetConfig,
#         **kwargs: Any,
#     ) -> xr.Dataset:
#         raise NotImplementedError(
#             "ImprovedDefaultRetriever does not implement the 'retrieve' method"
#         )
