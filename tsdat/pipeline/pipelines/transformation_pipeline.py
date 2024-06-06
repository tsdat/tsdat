from typing import Any, Dict, List

import xarray as xr
from pydantic import BaseModel

from tsdat.io.retrievers import StorageRetriever
from tsdat.utils import decode_cf

from .ingest_pipeline import IngestPipeline
from .add_inputs_attr import add_inputs_attr


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
        add_inputs_attr(dataset, input_keys)
        dataset = self.prepare_retrieved_dataset(dataset)
        dataset = self.hook_customize_dataset(dataset)
        dataset = self.quality.manage(dataset)
        dataset = self.hook_finalize_dataset(dataset)
        # HACK: Fix encoding on datetime64 variables. Use a shallow copy to retain units
        # on datetime64 variables in the pipeline (but remove with decode_cf())
        dataset = decode_cf(dataset)
        self.storage.save_data(dataset)
        with self.storage.uploadable_dir() as tmp_dir:
            self._ds = dataset
            self._tmp_dir = tmp_dir
            self.hook_plot_dataset(dataset)
        return dataset

    @staticmethod
    def hook_customize_input_datasets(
        input_datasets: Dict[str, xr.Dataset], **kwargs: Any
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
