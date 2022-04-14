# TODO: Implement MultiDatastreamRetriever
import logging
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Dict, List
from tsdat.config.dataset import DatasetConfig

from .base import Retriever, DataReader, DataConverter


logger = logging.getLogger(__name__)


# class AttributeRetrieverConfig(BaseModel, extra=Extra.forbid):
#     ...


class VariableRetrieverConfig(BaseModel, extra=Extra.forbid):
    name: str
    # required: bool = True
    data_converters: List[DataConverter] = []


class SimpleRetriever(Retriever):
    class Parameters(BaseModel, extra=Extra.forbid):
        # TODO: attrs: Optional[AttributeRetrieverConfig]
        coords: Dict[str, VariableRetrieverConfig]
        data_vars: Dict[str, VariableRetrieverConfig]

    parameters: Parameters
    readers: Dict[str, DataReader]

    def retrieve(self, input_keys: List[str]) -> Dict[str, xr.Dataset]:
        """-----------------------------------------------------------------------------
        Retrieves the dataset(s) as a mapping like {input_key: xr.Dataset} using the
        registered DataReaders for the retriever.

        Args:
            input_keys (List[str]): The input keys the registered DataReaders should
            read from.

        Returns:
            Dict[str, xr.Dataset]: The raw dataset mapping.

        -----------------------------------------------------------------------------"""
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
            for reader in self.readers.values():
                if reader.matches(input_key):
                    input_reader_mapping[input_key] = reader
                    break
        return input_reader_mapping

    def prepare(
        self, raw_mapping: Dict[str, xr.Dataset], dataset_config: DatasetConfig
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Prepares the raw dataset mapping for use in downstream pipeline processes by
        consolidating the data into a single xr.Dataset object consisting only of
        variables specified by retriever configurations. Applies input data converters
        as part of the preparation process.

        Args:
            raw_mapping (Dict[str, xr.Dataset]): The raw dataset mapping (as returned by
            the 'retrieve' method.)

        Returns:
            xr.Dataset: The dataset

        -----------------------------------------------------------------------------"""
        raw_dataset = self._merge_raw_mapping(raw_mapping)
        dataset = self._reduce_raw_dataset(raw_dataset)
        dataset = self._run_data_converters(dataset, dataset_config)
        dataset = self._reindex_dataset_coords(dataset)
        return dataset

    def _merge_raw_mapping(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        # IDEA: Make this configurable or easily overrideable.
        return xr.merge(list(raw_mapping.values()))  # type: ignore

    def _reduce_raw_dataset(self, raw_dataset: xr.Dataset) -> xr.Dataset:
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
            c.name: output_name for output_name, c in self.parameters.coords.items()
        }
        vars_to_rename = {
            v.name: output_name for output_name, v in self.parameters.data_vars.items()
        }
        to_rename.update(coords_to_rename)
        to_rename.update(vars_to_rename)

        # Remove data variables we do not need to speed up other operations
        existing_vars = set(raw_dataset.variables)
        vars_to_drop = existing_vars - set(to_rename)
        raw_dataset = raw_dataset.drop_vars(vars_to_drop)

        # Error handling
        for raw_name, output_name in coords_to_rename.items():
            if raw_name not in raw_dataset:
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
            if raw_name not in raw_dataset:
                to_rename.pop(raw_name)
                logger.warn(
                    "Data variable '%s' could not be retrieved from input. Please"
                    " ensure the retrieval configuration file for the '%s' data"
                    " variable has the 'name' property set to the exact name of the"
                    " variable in the dataset returned by the input DataReader.",
                    raw_name,
                    output_name,
                )
        return raw_dataset.rename(to_rename)

    def _run_data_converters(
        self, dataset: xr.Dataset, dataset_config: DatasetConfig
    ) -> xr.Dataset:
        for coord_name, coord_config in self.parameters.coords.items():
            for converter in coord_config.data_converters:
                dataset = converter.convert(dataset, dataset_config, coord_name)
        for var_name, var_config in self.parameters.data_vars.items():
            for converter in var_config.data_converters:
                dataset = converter.convert(dataset, dataset_config, var_name)
        return dataset

    def _reindex_dataset_coords(self, dataset: xr.Dataset) -> xr.Dataset:
        # TODO: Reindex the dataset so that the specified coordinates are the new
        # dimensions & coords of the dataset.
        ...
