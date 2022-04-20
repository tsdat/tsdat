import re
from typing import Dict
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from pytest import fixture
from tsdat.config.dataset import DatasetConfig
from tsdat.io.retrievers import DefaultRetriever
from tsdat.io.readers import CSVReader
from tsdat.io.converters import StringToDatetime, UnitsConverter
from test.utils import assert_close


@fixture
def simple_retriever() -> DefaultRetriever:
    return DefaultRetriever(
        readers={"csv": CSVReader()},
        coords={
            "time": {
                re.compile(r".*"): {
                    "name": "timestamp",
                    "data_converters": [
                        StringToDatetime(format="%Y-%m-%d %H:%M:%S", timezone="UTC")
                    ],
                }
            }
        },  # type: ignore
        data_vars={
            "first": {
                re.compile(r".*"): {
                    "name": "First Data Var",
                    "data_converters": [UnitsConverter(input_units="degF")],
                }
            }
        },  # type: ignore
    )


@fixture
def single_raw_mapping() -> Dict[str, xr.Dataset]:
    return {
        "test/io/data/input.csv": xr.Dataset(
            coords={"index": ([0, 1, 2])},
            data_vars={
                "timestamp": (
                    "index",
                    [
                        "2022-03-24 21:43:00",
                        "2022-03-24 21:44:00",
                        "2022-03-24 21:45:00",
                    ],
                ),
                "First Data Var": (
                    "index",
                    [71.4, 71.2, 71.1],
                ),
            },
        ),
    }


@fixture
def multifile_single_variable_mapping(
    single_raw_mapping: Dict[str, xr.Dataset]
) -> Dict[str, xr.Dataset]:
    new_ds_mapping = {
        "test/io/test_retrievers.py:multifile_mapping": xr.Dataset(
            coords={"index": ([0, 1, 2])},
            data_vars={
                "timestamp": (
                    "index",
                    [
                        "2022-03-24 21:46:00",
                        "2022-03-24 21:47:00",
                        "2022-03-24 21:48:00",
                    ],
                ),
                "First Data Var": (
                    "index",
                    [71.0, 70.8, 70.6],
                ),
            },
        ),
    }
    return {**single_raw_mapping, **new_ds_mapping}


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
