# TODO: Implement SimpleRetriever
# TODO: Implement MultiDatastreamRetriever
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Dict, List, Optional
from .base import Retriever, DataReader, DataConverter


class AttributeRetrieverConfig(BaseModel, extra=Extra.forbid):
    ...


class VariableRetrieverConfig(BaseModel, extra=Extra.forbid):
    name: str
    data_converters: List[DataConverter] = []


class SimpleRetriever(Retriever):
    class Parameters(BaseModel, extra=Extra.forbid):
        attrs: Optional[AttributeRetrieverConfig]
        coords: Dict[str, VariableRetrieverConfig]
        data_vars: Dict[str, VariableRetrieverConfig]

    parameters: Parameters
    readers: Dict[str, DataReader]

    def retrieve_raw_datasets(self, input_keys: List[str]) -> Dict[str, xr.Dataset]:
        dataset_mapping: Dict[str, xr.Dataset] = {}
        input_reader_mapping = self._match_inputs_with_readers(input_keys)

        # IDEA: read asynchronously
        for input_key, reader in input_reader_mapping.items():
            data = reader.read(input_key)
            if isinstance(data, xr.Dataset):
                data = {input_key: data}
            dataset_mapping.update(data)
        return dataset_mapping

    def merge_raw_datasets(self, raw_mapping: Dict[str, xr.Dataset]) -> xr.Dataset:
        # TODO: Better name for this function?
        ...

    def _match_inputs_with_readers(
        self, input_keys: List[str]
    ) -> Dict[str, DataReader]:
        input_reader_mapping: Dict[str, DataReader] = {}
        for input_key in input_keys:
            for reader in self.readers.values():
                if reader.matches(input_key):
                    input_reader_mapping[input_key] = reader
                    break
        return input_reader_mapping

    def _convert_raw_dataset(self, raw_dataset: xr.Dataset) -> xr.Dataset:
        # Applies all registered converters to the dataset
        ...
