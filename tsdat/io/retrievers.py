# TODO: Implement SimpleRetriever
# TODO: Implement MultiDatastreamRetriever
import xarray as xr
from pydantic import BaseModel, Extra
from typing import Dict, List, Optional
from tsdat.config.dataset import DatasetConfig
from .base import Retriever, DataReader, DataConverter


class AttributeRetrieverConfig(BaseModel, extra=Extra.forbid):
    ...


class VariableRetrieverConfig(BaseModel, extra=Extra.forbid):
    name: str
    data_converters: List[DataConverter] = []


class SimpleRetrieverParameters(BaseModel, extra=Extra.forbid):
    attrs: Optional[AttributeRetrieverConfig]
    coords: Dict[str, VariableRetrieverConfig]
    data_vars: Dict[str, VariableRetrieverConfig]


class SimpleRetriever(Retriever):

    readers: Dict[str, DataReader]

    def retrieve_raw_datasets(
        self, input_keys: List[str], dataset_config: DatasetConfig
    ) -> Dict[str, xr.Dataset]:
        ...

    def merge_raw_datasets(
        self, raw_dataset_mapping: Dict[str, xr.Dataset], dataset_config: DatasetConfig
    ) -> xr.Dataset:
        ...
