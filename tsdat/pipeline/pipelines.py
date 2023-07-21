from typing import Any, Dict, List

import xarray as xr
from pydantic import BaseModel

from tsdat.io.retrievers import StorageRetriever
from tsdat.utils import decode_cf

from .base import Pipeline

__all__ = ["IngestPipeline", "TransformationPipeline"]


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


class TransformationPipeline(IngestPipeline):
    """---------------------------------------------------------------------------------
    Pipeline class designed to read in standardized time series data and enhance
    its quality and usability by combining multiple sources of data, using higher-level
    processing techniques, etc.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel):
        datastreams: List[str]
        """A list of datastreams that the pipeline should be configured to run for.
        Datastreams should include the location and data level information."""

    parameters: Parameters
    retriever: StorageRetriever

    def run(self, inputs: List[str], **kwargs: Any) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Runs the data pipeline on the provided inputs.

        Args:
            inputs (List[str]): A 2-element list of start-date, end-date that the
                pipeline should process.

        Returns:
            xr.Dataset: The processed dataset.

        -----------------------------------------------------------------------------"""
        if len(inputs) != 2:
            raise ValueError(
                f"'inputs' argument for {self.__repr_name__()}.run(inputs) must be a"
                f" two-element list of [start date, end date]. Got '{inputs}'"
            )

        # Build the input strings for the retriever, which uses a format like:
        # datastream::start::end, e.g., 'sgp.aosacsm.b1::20230101::20230102'
        start, end = inputs[0], inputs[1]
        input_keys = [
            f"{datastream}::{start}::{end}"
            for datastream in self.parameters.datastreams
        ]

        dataset = self.retriever.retrieve(
            input_keys,
            dataset_config=self.dataset_config,
            storage=self.storage,
            input_data_hook=self.hook_customize_input_datasets,
            **kwargs,
        )
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

    def hook_customize_input_datasets(
        self, input_datasets: Dict[str, xr.Dataset], **kwargs: Any
    ) -> Dict[str, xr.Dataset]:
        """-----------------------------------------------------------------------------
        Code hook to customize any input datasets prior to datastreams being combined
        and data converters being run.

        Args:
            input_datasets (Dict[str, xr.Dataset]): The dictionary of input key (str) to
                input dataset. Note that for transformation pipelines, input keys !=
                input filename, rather each input key is a combination of the datastream
                and date range used to pull the input data from the storage retriever.

        Returns:
            Dict[str, xr.Dataset]: The customized input datasets.

        -----------------------------------------------------------------------------"""
        return input_datasets
