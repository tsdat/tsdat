from pathlib import Path
from typing import Any, List, Optional

import xarray as xr
from pydantic import PrivateAttr

from tsdat.utils import decode_cf

from .add_inputs_attr import add_inputs_attr
from ..base import Pipeline


class IngestPipeline(Pipeline):
    """---------------------------------------------------------------------------------
    Pipeline class designed to read in raw, unstandardized time series data and enhance
    its quality and usability by converting it into a standard format, embedding
    metadata, applying quality checks and controls, generating reference plots, and
    saving the data in an accessible format so it can be used later in scientific
    analyses or in higher-level tsdat Pipelines.

    ---------------------------------------------------------------------------------"""

    _ds: Optional[xr.Dataset] = PrivateAttr(default=None)
    _tmp_dir: Optional[Path] = PrivateAttr(default=None)

    @property
    def ds(self) -> Optional[xr.Dataset]:
        return self._ds

    @property
    def tmp_dir(self) -> Optional[Path]:
        return self._tmp_dir

    def run(self, inputs: List[str], **kwargs: Any) -> xr.Dataset:
        dataset = self.retriever.retrieve(inputs, self.dataset_config)
        dataset = self.prepare_retrieved_dataset(dataset)
        add_inputs_attr(dataset, inputs)
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
    def hook_customize_dataset(dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Code hook to customize the retrieved dataset prior to qc being applied.

        Args:
            dataset (xr.Dataset): The output dataset structure returned by the retriever
                API.

        Returns:
            xr.Dataset: The customized dataset.

        -----------------------------------------------------------------------------"""
        return dataset

    @staticmethod
    def hook_finalize_dataset(dataset: xr.Dataset) -> xr.Dataset:
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

    def get_ancillary_filepath(
        self, title: str, extension: str = "png", **kwargs: Any
    ) -> Path:
        """Returns the path to where an ancillary file should be saved so that it can be
        synced to the storage area automatically.

        Args:
            title (str): The title to use for the plot filename. Should only contain
                alphanumeric and '_' characters.
            extension (str, optional): The file extension. Defaults to "png".

        Returns:
            Path: The ancillary filepath.
        """
        dataset = kwargs.pop("dataset", self.ds)
        root_dir = kwargs.pop("root_dir", self.tmp_dir)
        return self.storage.get_ancillary_filepath(
            title=title,
            extension=extension,
            dataset=dataset,
            root_dir=root_dir,
            **kwargs,
        )
