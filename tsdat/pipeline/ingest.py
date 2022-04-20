import xarray as xr
from typing import List, Set, cast
from tsdat.pipeline.pipeline import Pipeline
from tsdat.utils import decode_cf


class IngestPipeline(Pipeline):
    """---------------------------------------------------------------------------------
    Pipeline class designed to read in raw, unstandardized time series data and enhance
    its quality and usability by converting it into a standard format, embedding
    metadata, applying quality checks and controls, generating reference plots, and
    saving the data in an accessible format so it can be used later in scientific
    analyses or in higher-level tsdat Pipelines.

    ---------------------------------------------------------------------------------"""

    def run(self, inputs: List[str]) -> xr.Dataset:
        dataset = self.retriever.retrieve(inputs, self.dataset_config)
        dataset = self.generate_output_structure(dataset)
        dataset = self.hook_customize_dataset(dataset)
        dataset = self.quality.manage(dataset)
        dataset = self.hook_finalize_dataset(dataset)
        dataset = decode_cf(dataset)  # HACK: fixes the encoding on datetime64 variables
        self.storage.save_data(dataset)
        self.hook_plot_dataset(dataset)
        return dataset

    def hook_customize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        User-overrideable code hook that runs after the retriever has retrieved the
        dataset from the specified input keys, but before the pipeline has applied any
        quality checks or corrections to the dataset.

        Args:
            dataset (xr.Dataset): The output dataset structure returned by the retriever
            API.

        Returns:
            xr.Dataset: The customized dataset.

        -----------------------------------------------------------------------------"""
        return dataset

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        User-overrideable code hook that runs after the dataset quality has been managed
        but before the dataset has been sent to the storage API to be saved.

        Args:
            dataset (xr.Dataset): The output dataset returned by the retriever API and
            modified by the `hook_customize_retrieved_dataset` user code hook.

        Returns:
            xr.Dataset: The finalized dataset, ready to be saved.

        -----------------------------------------------------------------------------"""
        return dataset

    def hook_plot_dataset(self, dataset: xr.Dataset):
        """------------------------------------------------------------------------------------
        User-overrideable code hook that runs after the dataset has been saved by the storage
        API.

        Args:
            dataset (xr.Dataset): The dataset to plot.

        ------------------------------------------------------------------------------------"""
        pass

    def generate_output_structure(self, dataset: xr.Dataset) -> xr.Dataset:
        # Drop retrieved variables not in the DatasetConfig
        # Adds static variables to the dataset
        # Initializes variables that are not retrieved
        # Adds variable and global attributes to the dataset

        output_vars = set(self.dataset_config.coords) | set(
            self.dataset_config.data_vars
        )
        retrieved_vars = cast(Set[str], set(dataset.variables))

        vars_to_drop = retrieved_vars - output_vars
        vars_to_add = output_vars - retrieved_vars

        dataset.drop_vars(vars_to_drop)

        for name in vars_to_add:
            dims = self.dataset_config[name].dims

        return dataset
