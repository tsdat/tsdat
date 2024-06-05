import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pytest import fixture

from tsdat import (
    DatasetConfig,
    DefaultRetriever,
    FileSystem,
    RetrieverConfig,
    StorageRetriever,
    StorageRetrieverInput,
    assert_close,
    recursive_instantiate,
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
def storage_retriever_transform() -> StorageRetriever:
    config = RetrieverConfig.from_yaml(
        Path("test/io/yaml/vap-retriever-transform.yaml")
    )
    return recursive_instantiate(config)


@fixture
def storage_retriever_fetch() -> StorageRetriever:
    config = RetrieverConfig.from_yaml(Path("test/io/yaml/vap-retriever-fetch.yaml"))
    return recursive_instantiate(config)


@fixture
def vap_dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/io/yaml/vap-dataset.yaml"))


@fixture
def vap_dataset_config_2D() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/io/yaml/vap-dataset-2D.yaml"))


@fixture
def vap_transform_dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/io/yaml/vap-dataset-transform.yaml"))


@fixture
def dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))


def test_storage_retriever_input_key():
    # old format
    key = "sgp.testing.c1::20230801::20230901.120000"
    obj = StorageRetrieverInput(key)
    assert obj.datastream == "sgp.testing.c1"
    assert obj.start.strftime("%Y%m%d") == "20230801"
    assert obj._end == "20230901.120000"

    # new format
    datastream = "--datastream sgp.testing.c1"
    start, end = "--start 20230801", "--end 20230901"
    location = "--location_id sgp"
    key = f"{datastream} {start} {end} {location}"
    obj = StorageRetrieverInput(key)
    assert obj.datastream == "sgp.testing.c1"
    assert obj.start.strftime("%Y%m%d") == "20230801"
    assert obj._end == "20230901"
    assert repr(obj) == (
        "StorageRetrieverInput(datastream=sgp.testing.c1, start=20230801, end=20230901,"
        " location_id=sgp)"
    )

    # new format, error
    key = "--datastream sgp.testing.c1 20230801 20230901"  # missing ids
    with pytest.raises(ValueError):
        StorageRetrieverInput(key)


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


@pytest.mark.requires_adi
def test_storage_retriever(
    storage_retriever: StorageRetriever, vap_dataset_config: DatasetConfig
):
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path("test/io/data/retriever-store"),
            data_storage_path="data/{datastream}",
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
            "qc_temperature": ("time", [0, 0, 0]),
            "qc_humidity": ("time", [0, 0, 0]),
        },
        attrs={"datastream": "humboldt.buoy.b1"},
    )

    xr.testing.assert_allclose(retrieved_dataset, expected)  # type: ignore


def test_storage_retriever_2D(
    storage_retriever_2D: StorageRetriever, vap_dataset_config_2D: DatasetConfig
):
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path("test/io/data/retriever-store"),
            data_storage_path="data/{datastream}",
        )
    )
    inputs = [
        "--datastream humboldt.buoy_z06-2D.a1 --start 20220405.000000 --end 20220406.000000",
        "--datastream humboldt.buoy_z07-2D.a1 --start 20220405.000000 --end 20220406.000000",
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
            "dummy": ("dim_0", []),
        },
        attrs={"datastream": "humboldt.buoy.b1"},
    )

    xr.testing.assert_allclose(retrieved_dataset, expected)  # type: ignore


@pytest.mark.requires_adi
def test_storage_retriever_transformations(
    storage_retriever_transform: StorageRetriever,
    vap_transform_dataset_config: DatasetConfig,
):
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path("test/io/data/retriever-store"),
            data_storage_path="data/{datastream}",
        )
    )

    input_dataset = xr.Dataset(
        coords={
            "time": pd.to_datetime(
                [
                    "2022-04-13 14:00:00",  # 0
                    "2022-04-13 14:10:00",  # 1
                    "2022-04-13 14:20:00",  # 2
                    "2022-04-13 14:30:00",  # -9999
                    "2022-04-13 14:40:00",  # 4
                    "2022-04-13 14:50:00",  # 5
                ]
            )
        },
        data_vars={
            "time_bounds": (
                ("timestamp", "bound"),
                (
                    [
                        pd.to_datetime(["2022-04-13 13:55:00", "2022-04-13 14:05:00"]),
                        pd.to_datetime(["2022-04-13 14:05:00", "2022-04-13 14:15:00"]),
                        pd.to_datetime(["2022-04-13 14:15:00", "2022-04-13 14:25:00"]),
                        pd.to_datetime(["2022-04-13 14:25:00", "2022-04-13 14:35:00"]),
                        pd.to_datetime(["2022-04-13 14:35:00", "2022-04-13 14:45:00"]),
                        pd.to_datetime(["2022-04-13 14:45:00", "2022-04-13 14:55:00"]),
                    ]
                ),
                {"comment": "bounds for time variable"},
            ),
            "temp": (
                "time",
                [0.0, 1.0, 2.0, -9999.0, 4.0, 5.0],
                {"units": "degC", "_FillValue": -9999},
            ),
            "qc_temp": (
                "time",
                [0, 0, 0, 1, 0, 0],
                {
                    "flag_values": "1",
                    "flag_assessments": "Bad",
                    "flag_meanings": "Value_equal_to_missing_value",
                },
            ),
            "rh": (
                "time",
                [59, 60, 61, 62, 63, 64],
                {"comment": "test case with no units attr"},
            ),
        },
        attrs={"datastream": "test.trans_inputs.a1"},
    )
    path = Path(
        "test/io/data/retriever-store/data/test.trans_inputs.a1/test.trans_inputs.a1.20220413.140000.nc"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    input_dataset.to_netcdf(path)  # type: ignore
    inputs = [
        "--datastream test.trans_inputs.a1 --start 20220413.000000 --end 20220414.000000",
    ]

    ds = storage_retriever_transform.retrieve(
        inputs, dataset_config=vap_transform_dataset_config, storage=storage
    )

    for var in [
        "temperature_5min",
        "temperature_30min",
        "temperature_60min",
        "humidity",
    ]:
        assert (
            var in ds.data_vars
        ), f"{var} is expected to be in dataset. Found: {list(ds)}"
        assert (
            f"qc_{var}" in ds.data_vars
        ), f"qc_{var} is expected to be in dataset. Found: {list(ds)}"

    t5min = ds["temperature_5min"]
    assert "TRANS_INTERPOLATE" in t5min.attrs.get("cell_transform", "")
    np.testing.assert_equal(
        t5min.sel(  # type: ignore
            time=slice("2022-04-13 13:50:00", "2022-04-13 15:00:00")
        ).values,
        np.array([-9999, -0.5, 0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, -9999]),
    )

    t30min = ds["temperature_30min"]
    assert "TRANS_BIN_AVERAGE" in t30min.attrs.get("cell_transform", "")
    np.testing.assert_equal(
        t30min.sel(  # type: ignore
            time_30min=slice("2022-04-13 13:30:00", "2022-04-13 15:30:00")
        ).values,
        np.array([-9999, 0, 1.2, 4.5, -9999]),
    )

    t60min = ds["temperature_60min"]
    assert "TRANS_BIN_AVERAGE" in t60min.attrs.get("cell_transform", "")
    np.testing.assert_equal(
        t60min.sel(  # type: ignore
            time_60min=slice("2022-04-13 12:00:00", "2022-04-13 15:00:00")
        ).values,
        np.array([-9999, 0, 8 / 3, -9999]),
    )

    humidity = ds["humidity"]
    assert "TRANS_SUBSAMPLE" in humidity.attrs.get("cell_transform", "")
    np.testing.assert_equal(
        humidity.sel(  # type: ignore
            time=slice("2022-04-13 13:40:00", "2022-04-13 15:10:00")
        ).values,
        np.array(
            [
                -9999,
                59,
                59,
                59,
                59,
                59,
                60,
                60,
                61,
                61,
                62,
                62,
                63,
                63,
                64,
                64,
                64,
                64,
                -9999,
            ]
        ),
    )

    os.remove(path)


@pytest.mark.requires_adi
def test_storage_retriever_file_fetching(
    storage_retriever_fetch: StorageRetriever, vap_dataset_config: DatasetConfig
):
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path("test/io/data/retriever-store"),
            data_storage_path="data/{datastream}",
        )
    )

    inputs = [
        "humboldt.buoy_z06.a1::20220405.000000::20220406.000000",
    ]

    retrieved_dataset = storage_retriever_fetch.retrieve(
        inputs, dataset_config=vap_dataset_config, storage=storage
    )

    time = np.append(
        np.arange(
            np.datetime64("2022-04-05T00:00:00"),
            np.datetime64("2022-04-06T00:00:00"),
            np.timedelta64(8, "h"),
        ),
        np.datetime64("2022-04-05T20:00:00"),
    )
    expected = xr.Dataset(
        coords={"time": time},  # type: ignore
        data_vars={
            "temperature": (  # degF -> degC
                "time",
                (np.array([71.4, 71.2, 71.1, 70.5]) - 32) * 5 / 9,
                {"units": "degC"},
            ),
            "qc_temperature": ("time", [0, 0, 0, 0]),
        },
        attrs={"datastream": "humboldt.buoy.b1"},
    )

    xr.testing.assert_allclose(retrieved_dataset, expected)  # type: ignore
