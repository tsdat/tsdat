import os
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
from pytest import fixture

from tsdat import (
    DatasetConfig,
    FileSystem,
    RetrieverConfig,
    StorageRetriever,
    recursive_instantiate,
)

# Coords used in sample input data
time_3pt = pd.date_range("2022-04-05", "2022-04-06", periods=3 + 1, inclusive="left")  # type: ignore
time_10pt = pd.date_range("2022-04-05", "2022-04-06", periods=10 + 1, inclusive="left")  # type: ignore

height_3pt = [0.0, 5.0, 10.0]
height_4pt = [1.0, 4.0, 11.0, 17.0]


# #################################################################################### #
#                                       FIXTURES
#
@fixture
def vap_transform_dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/io/yaml/vap-dataset-transform.yaml"))


@fixture
def storage_retriever_v2_transform() -> StorageRetriever:
    config = RetrieverConfig.from_yaml(
        Path("test/io/yaml/vap-retriever-v2-transform.yaml")
    )
    return recursive_instantiate(config)


# #################################################################################### #
#                                       TESTS
#
def create_input_dataset(filepath: str | Path) -> xr.Dataset:
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
            "qc_rh": (
                "time",
                [0, 0, 0, 0, 0, 0],
                {
                    "flag_values": "1",
                    "flag_assessments": "Bad",
                    "flag_meanings": "Value_equal_to_missing_value",
                },
            ),
        },
        attrs={"datastream": "test.trans_inputs.a1"},
    )
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    input_dataset.to_netcdf(path)  # type: ignore

    return input_dataset


def test_transform_v2(
    storage_retriever_v2_transform: StorageRetriever,
    vap_transform_dataset_config: DatasetConfig,
):
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path("test/io/data/retriever-store"),
            data_storage_path="data/{datastream}",  # type: ignore
        )
    )

    input_path = "test/io/data/retriever-store/data/test.trans_inputs.a1/test.trans_inputs.a1.20220413.140000.nc"
    _ = create_input_dataset(input_path)

    inputs = [
        "--datastream test.trans_inputs.a1 --start 20220413.000000 --end 20220414.000000",
    ]

    ds = storage_retriever_v2_transform.retrieve(
        inputs, dataset_config=vap_transform_dataset_config, storage=storage
    )

    for var in [
        # "temperature_5min",
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

    t30min = ds["temperature_30min"]
    # assert "TRANS_BIN_AVERAGE" in t30min.attrs.get("cell_transform", "")
    np.testing.assert_equal(
        t30min.sel(  # type: ignore
            time_30min=slice("2022-04-13 13:30:00", "2022-04-13 15:30:00")
        )
        .fillna(-9999)
        .values,
        np.array([-9999, 0, 1.2, 4.5, -9999]),
    )

    t60min = ds["temperature_60min"]
    np.testing.assert_equal(
        t60min.sel(  # type: ignore
            time_60min=slice("2022-04-13 12:00:00", "2022-04-13 15:00:00")
        )
        .fillna(-9999)
        .values,
        np.array([-9999, 0, 8 / 3, -9999]),
    )

    humidity = ds["humidity"]
    np.testing.assert_equal(
        humidity.sel(  # type: ignore
            time=slice("2022-04-13 13:40:00", "2022-04-13 15:10:00")
        )
        .fillna(-9999)
        .values,
        np.array(
            [
                -9999,
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
                64,
                -9999,
            ]
        ),
    )

    os.remove(input_path)
