import xarray as xr

from typing import Dict, List
from tsdat.pipeline.pipeline import Pipeline
from tsdat.utils import decode_cf


# TODO: Add docstrings to public methods


class IngestPipeline(Pipeline):
    def run(self, inputs: List[str]) -> xr.Dataset:
        dataset_dict = self.retriever.retrieve(inputs)
        dataset_dict = self.customize_raw_datasets(dataset_dict)
        dataset = self.retriever.extract_dataset(dataset_dict, self.dataset_config)

        dataset = self.customize_retrieved_dataset(dataset, dataset_dict)
        dataset = self.quality.manage(dataset)

        dataset = self.finalize_dataset(dataset, dataset_dict)
        dataset = decode_cf(dataset)
        self.storage.save_data(dataset)
        self.plot(dataset, dataset_dict)
        return dataset

    def customize_raw_datasets(
        self, dataset_dict: Dict[str, xr.Dataset]
    ) -> Dict[str, xr.Dataset]:
        return dataset_dict

    def customize_retrieved_dataset(
        self, dataset: xr.Dataset, dataset_dict: Dict[str, xr.Dataset] = {}
    ) -> xr.Dataset:
        return dataset

    def finalize_dataset(
        self, dataset: xr.Dataset, dataset_dict: Dict[str, xr.Dataset] = {}
    ) -> xr.Dataset:
        return dataset

    def plot(self, dataset: xr.Dataset, dataset_dict: Dict[str, xr.Dataset] = {}):
        pass
