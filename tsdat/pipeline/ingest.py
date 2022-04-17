import xarray as xr

from typing import Dict, List
from tsdat.pipeline.pipeline import Pipeline
from tsdat.utils import decode_cf


# TODO: Add docstrings to public methods


class IngestPipeline(Pipeline):
    """---------------------------------------------------------------------------------
    Pipeline class designed to read in raw, unstandardized time series data and enhance
    its quality and usability by converting it into a standard format, embedding
    metadata, applying quality checks and controls, generating reference plots, and
    saving the data in an accessible format so it can be used later in scientific
    analyses or in higher-level tsdat Pipelines.

    ---------------------------------------------------------------------------------"""

    def run(self, inputs: List[str]) -> xr.Dataset:
        dataset_dict = self.retriever.retrieve(inputs)
        dataset_dict = self.customize_raw_datasets(dataset_dict)
        dataset = self.retriever.extract_dataset(dataset_dict, self.dataset_config)

        dataset = self.customize_retrieved_dataset(dataset, dataset_dict)
        dataset = self.quality.manage(dataset)

        dataset = self.finalize_dataset(dataset, dataset_dict)
        dataset = decode_cf(dataset)  # HACK: fixes the encoding on datetime64 variables
        self.storage.save_data(dataset)
        self.plot(dataset, dataset_dict)
        return dataset

    def customize_raw_datasets(
        self, dataset_dict: Dict[str, xr.Dataset]
    ) -> Dict[str, xr.Dataset]:
        """-----------------------------------------------------------------------------
        User-overrideable code hook that runs after the raw dataset mapping is retrieved
        by the Retriever API. This code hook should modify the dataset mapping returned
        by the retriever, which will then be passed back into the retriever API with the
        output DatasetConfig to get a dataset in the output format.

        Args:
            dataset_dict (Dict[str, xr.Dataset]): The dataset mapping returned by the
            pipeline's Retriever API.

        Returns:
            Dict[str, xr.Dataset]: The modified dataset mapping.

        -----------------------------------------------------------------------------"""
        return dataset_dict

    def customize_retrieved_dataset(
        self, dataset: xr.Dataset, dataset_dict: Dict[str, xr.Dataset]
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        User-overrideable code hook that runs after the retriever has merged the raw
        dataset mapping into the output data structure, but before the pipeline has
        applied any quality checks or corrections to the dataset.

        Args:
            dataset (xr.Dataset): The output dataset structure returned by the retriever
            API.
            dataset_dict (Dict[str, xr.Dataset]): The dataset mapping returned by the
            Retriever API and modified by the `customize_raw_datasets` user code hook.

        Returns:
            xr.Dataset: The customized dataset.

        -----------------------------------------------------------------------------"""
        return dataset

    def finalize_dataset(
        self, dataset: xr.Dataset, dataset_dict: Dict[str, xr.Dataset] = {}
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        User-overrideable code hook that runs after the dataset quality has been managed
        but before the dataset has been sent to the Storage API to be saved.

        Args:
            dataset (xr.Dataset): The output dataset returned by the retriever API and
            modified by the `customize_retrieved_dataset` user code hook.
            dataset_dict (Dict[str, xr.Dataset]): The dataset mapping returned by the
            Retriever API and modified by the `customize_raw_datasets` user code hook.

        Returns:
            xr.Dataset: The finalized dataset, ready to be saved.

        -----------------------------------------------------------------------------"""
        return dataset

    def plot(self, dataset: xr.Dataset, dataset_dict: Dict[str, xr.Dataset] = {}):
        pass
