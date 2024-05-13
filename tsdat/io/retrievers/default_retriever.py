import logging
import xarray as xr
from pydantic import BaseModel, Extra
from typing import (
    Any,
    Dict,
    List,
    Pattern,
    cast,
)

from .input_key_retrieval_rules import InputKeyRetrievalRules
from ._reindex_dataset_coords import _reindex_dataset_coords
from ._rename_variables import _rename_variables
from ._run_data_converters import _run_data_converters
from ...config.dataset import DatasetConfig
from ..base import (
    DataReader,
    Retriever,
)

logger = logging.getLogger(__name__)


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
