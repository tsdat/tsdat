import xarray as xr

from typing import Dict, List

from tsdat.pipeline.base import AbstractPipeline
from tsdat.dsutils import decode_cf_wrapper


class IngestPipeline(AbstractPipeline):
    def run(self, inputs: List[str]) -> xr.Dataset:
        raw_dataset_mapping = self.handle_inputs(inputs)
        raw_dataset_mapping = self.hook_customize_raw_datasets(raw_dataset_mapping)
        dataset = self.standardize_dataset(raw_dataset_mapping)
        dataset = self.hook_customize_dataset(dataset, raw_dataset_mapping)
        dataset = self.handle_data_quality(dataset)
        dataset = self.hook_finalize_dataset(dataset, raw_dataset_mapping)
        dataset = self.handle_dataset_encodings(dataset)
        self.handle_dataset_persistance(dataset)
        self.hook_make_plots(dataset, raw_dataset_mapping)
        return dataset

    def handle_inputs(self, inputs: List[str]) -> Dict[str, xr.Dataset]:
        # TODO: Implement me
        # Relevant settings in self.settings and self.config.storage
        ...

    def hook_customize_raw_datasets(
        self, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> Dict[str, xr.Dataset]:
        return raw_dataset_mapping

    def standardize_dataset(
        self, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:
        # TODO: Implement me
        # we should focus on making this as performant as possible
        ...

    def hook_customize_dataset(
        self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:
        return dataset

    def handle_data_quality(self, dataset: xr.Dataset) -> xr.Dataset:
        # TODO: Implement me
        ...

    def hook_finalize_dataset(
        self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset]
    ) -> xr.Dataset:
        return dataset

    def handle_dataset_encodings(self, dataset: xr.Dataset) -> xr.Dataset:
        return decode_cf_wrapper(dataset)

    def handle_dataset_persistance(self, dataset: xr.Dataset):
        # TODO: Implement me
        ...

    def hook_make_plots(
        self, dataset: xr.Dataset, raw_dataset_mapping: Dict[str, xr.Dataset]
    ):
        pass
