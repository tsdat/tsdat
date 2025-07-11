from abc import ABC, abstractmethod
from datetime import datetime
from getpass import getuser
from pathlib import Path
from typing import Any, Iterable, List, Optional, Pattern, cast

import numpy as np
import xarray as xr
from pydantic import Field

from ...config.dataset import DatasetConfig
from ...io.base import Retriever, Storage
from ...qc.base import QualityManagement
from ...utils import ParameterizedClass, model_to_dict


class Pipeline(ParameterizedClass, ABC):
    """---
    Base class for tsdat data pipelines.

    ---
    """

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

    cfg_filepath: Optional[Path] = None
    """The pipeline.yaml file containing the parameters used to instantiate this object"""

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
        output_vars = (
            list(self.dataset_config.coords)
            + list(self.dataset_config.data_vars)
            + [v for v in dataset.data_vars if "qc_" in v]
        )
        retrieved_variables = cast(List[str], list(dataset.variables))
        vars_to_drop = [ret for ret in retrieved_variables if ret not in output_vars]
        vars_to_add = [out for out in output_vars if out not in retrieved_variables]

        dataset = dataset.drop_vars(vars_to_drop)
        dataset = self._add_dataset_dtypes(dataset)
        dataset = self._add_dataset_variables(dataset, vars_to_add)
        dataset = self._add_dataset_attrs(dataset, output_vars)
        # TODO: reorder dataset coords / data vars to match the order in the config file

        # BUG: can't carry through old QC variables
        dataset = self._force_drop_qc(dataset)

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

    def _add_dataset_dtypes(self, dataset: xr.Dataset) -> xr.Dataset:
        for name in dataset.data_vars:
            if ("qc" in name) and not hasattr(self.dataset_config, name):
                continue
            dtype = self.dataset_config[name].dtype  # type: ignore
            dataset[name] = dataset[name].astype(dtype)
            # Add QC variables (should be added in transform converter)
            if f"qc_{name}" in dataset:
                # Assuming missing values have a bit of 1
                dataset[f"qc_{name}"] = dataset[f"qc_{name}"].fillna(1).astype(np.int32)
        return dataset

    def _add_dataset_attrs(
        self, dataset: xr.Dataset, output_vars: Iterable[str]
    ) -> xr.Dataset:
        from tsdat import get_version

        global_attrs = model_to_dict(self.dataset_config.attrs)
        dataset.attrs.update(**global_attrs)

        for name in output_vars:
            if ("qc" in name) and not hasattr(self.dataset_config, name):
                # Change non-list flags to list
                for a in dataset[name].attrs:
                    attr = dataset[name].attrs[a]
                    if not (
                        isinstance(attr, list) or isinstance(attr, np.ndarray)
                    ) and ("flag" in a):
                        dataset[name].attrs[a] = [attr]
            else:
                var_attrs = model_to_dict(self.dataset_config[name].attrs)
                dataset[name].attrs.update(var_attrs)

        history = f"Created by {getuser()} at {datetime.now().isoformat()} using tsdat v{get_version()}"
        dataset.attrs["history"] = history

        return dataset

    def _force_drop_qc(self, dataset: xr.Dataset) -> xr.Dataset:
        """Drop QC variables since act-atmos isn't smart enough to see repeated tests"""
        qc_vars = [v for v in dataset.data_vars if "qc_" in v]
        dataset = dataset.drop_vars(qc_vars)
        return dataset
