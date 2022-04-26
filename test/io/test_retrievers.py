import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from pytest import fixture
from tsdat.config.dataset import DatasetConfig
from tsdat.config.retriever import RetrieverConfig
from tsdat.io.retrievers import DefaultRetriever
from tsdat.testing import assert_close
from tsdat.config.utils import recursive_instantiate


@fixture
def simple_retriever() -> DefaultRetriever:
    config = RetrieverConfig.from_yaml(Path("test/config/yaml/retriever.yaml"))
    return recursive_instantiate(config)


@fixture
def dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))


def test_simple_extract_dataset(
    simple_retriever: DefaultRetriever,
    dataset_config: DatasetConfig,
):
    expected = xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:45:00", periods=3),  # type: ignore
            ),
            "index": ("time", [0, 1, 2]),
        },
        data_vars={
            "first": (
                "time",
                (np.array([71.4, 71.2, 71.1]) - 32) * 5 / 9,  # type: ignore
                {"units": "degC"},
            )
        },
    )

    dataset = simple_retriever.retrieve(["test/io/data/input.csv"], dataset_config)
    assert_close(dataset, expected)


def test_simple_extract_multifile_dataset(
    simple_retriever: DefaultRetriever,
    dataset_config: DatasetConfig,
):
    expected = xr.Dataset(
        coords={
            "time": (
                "time",
                pd.date_range("2022-03-24 21:43:00", "2022-03-24 21:48:00", periods=6),  # type: ignore
            ),
            "index": ("time", [0, 1, 2, 0, 1, 2]),
        },
        data_vars={
            "first": (
                "time",
                (np.array([71.4, 71.2, 71.1, 71.0, 70.8, 70.6]) - 32) * 5 / 9,  # type: ignore
                {"units": "degC"},
            )
        },
    )
    dataset = simple_retriever.retrieve(
        ["test/io/data/input.csv", "test/io/data/input_extended.csv"], dataset_config
    )
    assert_close(dataset, expected)


def test_multi_datastream_retrieval():
    # TODO
    pass
