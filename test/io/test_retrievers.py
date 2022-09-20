import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from pytest import fixture
from tsdat import (
    DatasetConfig,
    RetrieverConfig,
    DefaultRetriever,
    StorageRetriever,
    FileSystem,
    recursive_instantiate,
    assert_close,
)


@fixture
def simple_retriever() -> DefaultRetriever:
    config = RetrieverConfig.from_yaml(Path("test/config/yaml/retriever.yaml"))
    return recursive_instantiate(config)


@fixture
def storage_retriever() -> StorageRetriever:
    config = RetrieverConfig.from_yaml(Path("test/io/yaml/vap-retriever.yaml"))
    return recursive_instantiate(config)


@fixture
def vap_dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/io/yaml/vap-dataset.yaml"))


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


def test_storage_retriever(
    storage_retriever: StorageRetriever, vap_dataset_config: DatasetConfig
):
    # TODO: Test with some 2D data
    storage = FileSystem(
        parameters=FileSystem.Parameters(storage_root=Path("test/io/retriever-store"))
    )
    inputs = [
        "humboldt.buoy_z06.a1::20220405.000000::20220406.000000",
        "humboldt.buoy_z07.a1::20220405.000000::20220406.000000",
    ]

    retrieved_dataset = storage_retriever.retrieve(
        inputs, dataset_config=vap_dataset_config, storage=storage
    )

    expected = xr.Dataset(
        coords={"time": pd.date_range("2022-04-05", "2022-04-06", periods=3 + 1, inclusive="left")},  # type: ignore
        data_vars={
            "temperature": (  # degF -> degC
                "time",
                (np.array([70, 76, 84]) - 32) * 5 / 9,
                {"units": "degC"},
            ),
            "humidity": ("time", [0, 30, 70]),
        },
        attrs={"datastream": "humboldt.buoy.b1"},
    )

    xr.testing.assert_allclose(retrieved_dataset, expected)  # type: ignore


# TEST: Multiple input datasets with non-overlapping or partially-overlapping data vars
# def test_multi_datastream_retrieval():
#     pass
