# TODO: Retrieval from S3; another retriever class, or parameters on the default?
# IDEA: Implement MultiDatastreamRetriever & variable finders

import logging
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Any, Dict, List, Pattern, cast
from ..config.dataset import DatasetConfig
from .base import Retriever, DataReader, DataConverter

__all__ = ["DefaultRetriever"]

logger = logging.getLogger(__name__)


class RetrievedVariable(BaseModel, extra=Extra.forbid):
    name: str
    data_converters: List[DataConverter] = []


class InputKeyRetrieverConfig:
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
            dataset = self._rename_variables(dataset, input_config)
            dataset = self._reindex_dataset_coords(
                dataset, dataset_config, input_config
            )
            dataset = self._run_data_converters(dataset, dataset_config, input_config)
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

    def _rename_variables(
        self,
        dataset: xr.Dataset,
        input_config: InputKeyRetrieverConfig,
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

    def _run_data_converters(
        self,
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

    def _reindex_dataset_coords(
        self,
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
        for coord_name in input_config.coords:
            expected_dim = dataset_config[coord_name].dims[0]
            actual_dims = dataset[coord_name].dims
            if (ndims := len(actual_dims)) != 1:
                raise ValueError(
                    f"Retrieved coordinate '{coord_name}' must have exactly one"
                    f" dimension in the retrieved dataset, found {ndims} (dims="
                    f"{actual_dims}). If '{coord_name}' is not actually a coordinate"
                    " variable, please move it to the data_vars section in the"
                    " retriever config file."
                )
            dim = actual_dims[0]
            if dim != expected_dim:
                dataset = dataset.swap_dims({dim: expected_dim})  # type: ignore

        return dataset

    def _merge_raw_mapping(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        return xr.merge(list(raw_mapping.values()), **self.parameters.merge_kwargs)  # type: ignore
