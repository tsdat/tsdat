# TODO: Retrieval from S3; another retriever class, or parameters on the default?
# TODO: Ensure correct data type and add attributes
# TODO: Variable finders

# Example use cases and logical routes:
#
# one file in, one file out
# data: {timestamp: {str, ["2022-04-18"]}, temp: {int, [80]}}
#
# 1. renaming
#       timestamp --> time,
#       temp --> temperature
# 2. converters
#       time str --> datetime64
#       temperature degF --> degC
# 3. xr.merge
#       [ds1] -> ds1
# 4. transforms
#       time (5min) --> 1 min, average

# two files in, one file out
# data: {timestamp: {str, ["2022-04-18"]}, temp: {int, [80]}}
# data: {timestamp: {str, ["2022-04-19"]}, temp: {int, [81]}}
#
# 1. renaming
#       timestamp --> time,
#       temp --> temperature
# 2. converters
#       time str --> datetime64
#       temperature degF --> degC
# 3. xr.merge
#       [ds1] -> ds1
# 4. transforms
#       time (5min) --> 1 min, average


# IDEA: Implement MultiDatastreamRetriever
import logging
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Any, Dict, List, Pattern
from tsdat.config.dataset import DatasetConfig

from .base import Retriever, DataReader, DataConverter


logger = logging.getLogger(__name__)


class VariableRetriever(BaseModel, extra=Extra.forbid):
    name: str
    data_converters: List[DataConverter] = []


class InputKeyRetrieverConfig:
    def __init__(self, input_key: str, retriever: "DefaultRetriever") -> None:
        self.coords: Dict[str, VariableRetriever] = {}
        self.data_vars: Dict[str, VariableRetriever] = {}

        def update_mapping(
            to_update: Dict[str, VariableRetriever],
            variable_dict: Dict[str, Dict[Pattern[str], VariableRetriever]],
        ):
            for name, retriever_dict in variable_dict.items():
                for pattern, variable_retriever in retriever_dict.items():
                    if pattern.match(input_key):
                        to_update[name] = variable_retriever
                    break

        update_mapping(self.coords, retriever.coords)  # type: ignore
        update_mapping(self.data_vars, retriever.data_vars)  # type: ignore


# TODO: Name this retriever something more apt
class DefaultRetriever(Retriever):
    class Parameters(BaseModel, extra=Extra.forbid):
        merge_kwargs: Dict[str, Any] = {}
        retain_global_attrs: bool = True
        retain_variable_attrs: bool = False
        # drop_unused_vars: bool = True

    parameters: Parameters = Parameters()

    readers: Dict[str, DataReader]
    """A dictionary of DataReaders that should be used to read data provided an input
    key."""

    coords: Dict[str, Dict[Pattern, VariableRetriever]]  # type: ignore
    """A dictionary mapping output coordinate names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each retrieved
    coordinate variable."""

    data_vars: Dict[str, Dict[Pattern, VariableRetriever]]  # type: ignore
    """A dictionary mapping output data variable names to the retrieval rules and
    preprocessing actions (e.g., DataConverters) that should be applied to each retrieved
    data variable."""

    def retrieve(
        self, input_keys: List[str], dataset_config: DatasetConfig
    ) -> xr.Dataset:
        # TODO: Parent docstring
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

    #     """-----------------------------------------------------------------------------
    #     Prepares the raw dataset mapping for use in downstream pipeline processes by
    #     consolidating the data into a single xr.Dataset object consisting only of
    #     variables specified by retriever configurations. Applies input data converters
    #     as part of the preparation process.

    #     Args:
    #         raw_mapping (Dict[str, xr.Dataset]): The raw dataset mapping (as returned by
    #         the 'retrieve' method.)
    #         dataset_config (DatasetConfig): The DatasetConfig used to gather info about
    #         the output dataset structure.

    #     Returns:
    #         xr.Dataset: The dataset

    #     -----------------------------------------------------------------------------"""

    def _match_inputs(self, input_keys: List[str]) -> Dict[str, DataReader]:
        input_reader_mapping: Dict[str, DataReader] = {}
        for input_key in input_keys:
            for reader in self.readers.values():
                if reader.matches(input_key):
                    input_reader_mapping[input_key] = reader
                    break
        return input_reader_mapping

    def _rename_variables(
        self,
        dataset: xr.Dataset,
        input_config: InputKeyRetrieverConfig,
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Returns a dataset containing only the variables specified to be retrieved in the
        retrieval config.

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

        # Warning handling
        for raw_name, output_name in coords_to_rename.items():
            if raw_name not in dataset:
                to_rename.pop(raw_name)
                logger.warn(
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
                logger.warn(
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
        Runs the declared DataConverters on the dataset's coords and data_vars. Returns
        the dataset after all converters have been run.

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
                # dataset = dataset.drop_vars([dim])

        return dataset

    # TODO: Move these methods into the pipeline class
    # def _drop_variables(self, dataset: xr.Dataset) -> xr.Dataset:
    #     """-----------------------------------------------------------------------------
    #     Drops variables from the dataset that are declared in neither the
    #     SimpleRetriever's coords nor data_vars sections.

    #     Args:
    #         dataset (xr.Dataset): The dataset to drop variables from.

    #     Returns:
    #         xr.Dataset: The dataset with undeclared variables and coordinates dropped.

    #     -----------------------------------------------------------------------------"""
    #     retriever_vars = set(self.coords) | set(self.data_vars)
    #     vars_to_drop = set(dataset.variables) - retriever_vars
    #     return dataset.drop_vars(vars_to_drop)

    # def _add_attrs(
    #     self, dataset: xr.Dataset, dataset_config: DatasetConfig
    # ) -> xr.Dataset:
    #     # Global attrs
    #     config_attrs = cast(
    #         Dict[Hashable, Any],
    #         dataset_config.attrs.dict(exclude_none=True, by_alias=True).copy(),
    #     )
    #     if self.parameters.retain_global_attrs:
    #         config_attrs.update(dataset.attrs)
    #         dataset.attrs = {**dataset.attrs, **config_attrs}
    #     dataset.attrs = config_attrs

    #     # Variable attrs
    #     for var_name in dataset.variables:
    #         if var_name in dataset_config:
    #             var_attrs = dataset_config[str(var_name)].attrs.dict(
    #                 exclude_none=True, by_alias=True
    #             )
    #             if self.parameters.retain_variable_attrs:
    #                 var_attrs.update(dataset[var_name].attrs)
    #             dataset[var_name].attrs = var_attrs

    #     return dataset

    def _merge_raw_mapping(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        return xr.merge(list(raw_mapping.values()), **self.parameters.merge_kwargs)  # type: ignore