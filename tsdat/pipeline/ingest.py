from abc import ABC, abstractmethod
from pydantic import BaseModel
import xarray as xr

from typing import Any, Dict, List
from tsdat.config.dataset import DatasetConfig
from tsdat.pipeline.pipeline import BasePipeline
from tsdat.utils import decode_cf_wrapper


class BaseRetriever(BaseModel, ABC):
    parameters: Any = {}

    @abstractmethod
    def fetch_inputs(self, inputs: Any):
        ...

    @abstractmethod
    def initialize_dataset(self, config: DatasetConfig) -> xr.Dataset:
        ...


class SimpleRetriever(BaseRetriever):
    def fetch_inputs(self, inputs: List[str]):
        print("Retrieving inputs!")

    def initialize_dataset(self, config: DatasetConfig) -> xr.Dataset:
        return super().initialize_dataset(config)


class IngestPipeline(BasePipeline):

    retriever: BaseRetriever = SimpleRetriever()

    def run(self, inputs: List[str]) -> xr.Dataset:
        # TODO: Retriever methods must return something, otherwise we must provide a way
        # to clean up any internal caches in between invocations of pipeline.run().
        self.retriever.fetch_inputs(inputs)
        self.hook_customize_raw_datasets()  # will access raw datasets on self.retriever directly
        dataset = self.retriever.initialize_dataset(
            self.dataset_config
        )  # takes the fetched inputs, uses dataset config
        # dataset = self.run_converters(dataset)
        dataset = self.hook_customize_dataset(dataset)
        dataset = self.handle_data_quality(dataset)
        dataset = self.hook_finalize_dataset(dataset)
        dataset = self.handle_dataset_encodings(dataset)
        self.handle_dataset_persistance(dataset)
        self.hook_make_plots(dataset)
        return dataset

    def handle_inputs(self, inputs: List[str]) -> Dict[str, xr.Dataset]:
        return self.storage.registry.read_all(inputs)

    def hook_customize_raw_datasets(self):
        # This should customize the raw datasets on self.retriever directly
        pass

    # def standardize_dataset(
    #     self, raw_dataset_mapping: Dict[str, xr.Dataset] = {}
    # ) -> xr.Dataset:
    #     # we should focus on making this as performant as possible
    #     ...

    def hook_customize_dataset(
        self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset] = {}
    ) -> xr.Dataset:
        return dataset

    def handle_data_quality(self, dataset: xr.Dataset) -> xr.Dataset:
        return self.quality.manage(dataset, self.dataset_config)

    def hook_finalize_dataset(
        self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset] = {}
    ) -> xr.Dataset:
        return dataset

    def handle_dataset_encodings(self, dataset: xr.Dataset) -> xr.Dataset:
        return decode_cf_wrapper(dataset)

    def handle_dataset_persistance(self, dataset: xr.Dataset):
        # TODO: Implement me
        ...

    def hook_make_plots(
        self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset] = {}
    ):
        pass
