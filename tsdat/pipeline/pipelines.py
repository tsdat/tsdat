import xarray as xr
from typing import Any, List

from tsdat.utils import decode_cf
from .base import Pipeline

__all__ = ["IngestPipeline"]


class IngestPipeline(Pipeline):
    """---------------------------------------------------------------------------------
    Pipeline class designed to read in raw, unstandardized time series data and enhance
    its quality and usability by converting it into a standard format, embedding
    metadata, applying quality checks and controls, generating reference plots, and
    saving the data in an accessible format so it can be used later in scientific
    analyses or in higher-level tsdat Pipelines.

    ---------------------------------------------------------------------------------"""

    def run(self, inputs: List[str], **kwargs: Any) -> xr.Dataset:
        dataset = self.retriever.retrieve(inputs, self.dataset_config)
        dataset = self.prepare_retrieved_dataset(dataset)
        dataset = self.hook_customize_dataset(dataset)
        dataset = self.quality.manage(dataset)
        dataset = self.hook_finalize_dataset(dataset)
        # HACK: Fix encoding on datetime64 variables. Use a shallow copy to retain units
        # on datetime64 variables in the pipeline (but remove with decode_cf())
        dataset = decode_cf(dataset)
        self.storage.save_data(dataset)
        self.hook_plot_dataset(dataset)
        return dataset

    def hook_customize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Code hook to customize the retrieved dataset prior to qc being applied.

        Args:
            dataset (xr.Dataset): The output dataset structure returned by the retriever
                API.

        Returns:
            xr.Dataset: The customized dataset.

        -----------------------------------------------------------------------------"""
        return dataset

    def hook_finalize_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Code hook to finalize the dataset after qc is applied but before it is saved.

        Args:
            dataset (xr.Dataset): The output dataset returned by the retriever API and
                modified by the `hook_customize_dataset` user code hook.

        Returns:
            xr.Dataset: The finalized dataset, ready to be saved.

        -----------------------------------------------------------------------------"""
        return dataset

    def hook_plot_dataset(self, dataset: xr.Dataset):
        """-----------------------------------------------------------------------------
        Code hook to create plots for the data which runs after the dataset has been saved.

        Args:
            dataset (xr.Dataset): The dataset to plot.

        -----------------------------------------------------------------------------"""
        pass
