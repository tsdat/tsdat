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

# Coords used in sample input data
time_3pt = pd.date_range("2022-04-05", "2022-04-06", periods=3 + 1, inclusive="left")  # type: ignore
time_10pt = pd.date_range("2022-04-05", "2022-04-06", periods=10 + 1, inclusive="left")  # type: ignore

height_3pt = [0.0, 5.0, 10.0]
height_4pt = [1.0, 4.0, 11.0, 17.0]


@fixture
def simple_retriever() -> DefaultRetriever:
    config = RetrieverConfig.from_yaml(Path("test/config/yaml/retriever.yaml"))
    return recursive_instantiate(config)


@fixture
def storage_retriever() -> StorageRetriever:
    config = RetrieverConfig.from_yaml(Path("test/io/yaml/vap-retriever.yaml"))
    return recursive_instantiate(config)


@fixture
def storage_retriever_2D() -> StorageRetriever:
    config = RetrieverConfig.from_yaml(Path("test/io/yaml/vap-retriever-2D.yaml"))
    return recursive_instantiate(config)


@fixture
def vap_dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/io/yaml/vap-dataset.yaml"))


@fixture
def vap_dataset_config_2D() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/io/yaml/vap-dataset-2D.yaml"))


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
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path("test/io/data/retriever-store")
        )
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


def test_storage_retriever_2D(
    storage_retriever_2D: StorageRetriever, vap_dataset_config_2D: DatasetConfig
):
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path("test/io/data/retriever-store")
        )
    )
    inputs = [
        "humboldt.buoy_z06-2D.a1::20220405.000000::20220406.000000",
        "humboldt.buoy_z07-2D.a1::20220405.000000::20220406.000000",
    ]

    retrieved_dataset = storage_retriever_2D.retrieve(
        inputs, dataset_config=vap_dataset_config_2D, storage=storage
    )

    expected = xr.Dataset(
        coords={
            "time": ("time", time_3pt),
            "height": ("height", height_3pt),
        },
        data_vars={
            "temperature": (  # degF -> degC
                ("time", "height"),
                (np.array([[0, 2, 4], [18, 20, 22], [42, 44, 46]]) - 32) * 5 / 9,
                {"units": "degC"},
            ),
            "humidity": ("time", [0, 10, 20]),
            "pressure": ("height", [15, 10, 5]),
        },
        attrs={"datastream": "humboldt.buoy.b1"},
    )

    xr.testing.assert_allclose(retrieved_dataset, expected)  # type: ignore
