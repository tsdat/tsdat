import numpy as np
import xarray as xr
from abc import ABC, abstractmethod
from getpass import getuser
from datetime import datetime
from typing import Any, Iterable, List, Pattern, cast
from pydantic import Field
from ..config.dataset import DatasetConfig
from ..io.base import Retriever, Storage
from ..qc.base import QualityManagement
from ..utils import ParameterizedClass, model_to_dict

__all__ = ["Pipeline"]


class Pipeline(ParameterizedClass, ABC):
    """------------------------------------------------------------------------------------
    Base class for tsdat data pipelines.

    ------------------------------------------------------------------------------------"""

    settings: Any = None

    triggers: List[Pattern] = []  # type: ignore
    """Regex patterns matching input keys to determine when the pipeline should run."""

    retriever: Retriever
    """Retrieves data from input keys."""

    dataset_config: DatasetConfig = Field(alias="dataset")
    """Describes the structure and metadata of the output dataset."""

    quality: QualityManagement
    """Manages the dataset quality through checks and corrections."""

    storage: Storage
    """Stores the dataset so it can be retrieved later."""

    @abstractmethod
    def run(self, inputs: List[str], **kwargs: Any) -> Any:
        """-----------------------------------------------------------------------------
        Runs the data pipeline on the provided inputs.

        Args:
            inputs (List[str]): A list of input keys that the pipeline's Retriever class
            can use to load data into the pipeline.

        Returns:
            xr.Dataset: The processed dataset.

        -----------------------------------------------------------------------------"""
        ...

    def prepare_retrieved_dataset(self, dataset: xr.Dataset) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Modifies the retrieved dataset by dropping variables not declared in the
        DatasetConfig, adding static variables, initializing non-retrieved variables,
        and importing global and variable-level attributes from the DatasetConfig.

        Args:
            dataset (xr.Dataset): The retrieved dataset.

        Returns:
            xr.Dataset: The dataset with structure and metadata matching the
            DatasetConfig.

        -----------------------------------------------------------------------------"""
        output_vars = set(self.dataset_config.coords) | set(
            self.dataset_config.data_vars
        )
        retrieved_vars = cast("set[str]", set(dataset.variables))
        vars_to_drop = retrieved_vars - output_vars
        vars_to_add = output_vars - retrieved_vars

        dataset = dataset.drop_vars(vars_to_drop)
        dataset = self._add_dataset_variables(dataset, vars_to_add)
        dataset = self._add_dataset_attrs(dataset, output_vars)
        # TODO: reorder dataset coords / data vars to match the order in the config file
        return dataset

    def _add_dataset_variables(
        self, dataset: xr.Dataset, vars_to_add: Iterable[str]
    ) -> xr.Dataset:
        for name in vars_to_add:
            dims = self.dataset_config[name].dims
            dtype = self.dataset_config[name].dtype
            data = self.dataset_config[name].data

            if data is None:
                fill_value = self.dataset_config[name].attrs.fill_value
                shape = tuple(len(dataset[d]) for d in dims)
                data = np.full(shape=shape, fill_value=fill_value, dtype=dtype)  # type: ignore
            else:
                # cast to specified data type. Note that np.array preserves scalars
                data = np.array(data, dtype=dtype)  # type: ignore

            dataset[name] = xr.DataArray(data=data, dims=dims)
        return dataset

    def _add_dataset_attrs(
        self, dataset: xr.Dataset, output_vars: Iterable[str]
    ) -> xr.Dataset:
        global_attrs = model_to_dict(self.dataset_config.attrs)
        dataset.attrs.update(**global_attrs)

        for name in output_vars:
            var_attrs = model_to_dict(self.dataset_config[name].attrs)
            dataset[name].attrs.update(var_attrs)

        history = f"Ran by {getuser()} at {datetime.now().isoformat()}"
        dataset.attrs["history"] = history

        return dataset
