from pathlib import Path
import xarray as xr
import pandas as pd
from pytest import fixture
from tsdat.config.dataset import DatasetConfig
from tsdat.io.converters import StringToDatetime, UnitsConverter
from test.utils import assert_close


@fixture
def sample_dataset() -> xr.Dataset:
    ds = xr.Dataset(
        coords={
            "time": (
                ["2022-04-13 14:10:00", "2022-04-13 14:20:00", "2022-04-13 14:30:00"]
            )
        },
        data_vars={
            "first": ("time", [59, 60, 61], {"units": "degF"}),
        },
    )
    return ds


@fixture
def dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))


def test_units_converter(sample_dataset: xr.Dataset, dataset_config: DatasetConfig):

    # Test using input units obtained from the 'raw' (sample) dataset
    expected = sample_dataset.assign(first=lambda x: (x.first - 32) * 5 / 9)  # type: ignore
    converter = UnitsConverter(input_units=None)
    dataset = converter.convert(sample_dataset, dataset_config, "first")
    assert_close(dataset, expected, check_attrs=False)
    assert dataset["first"].attrs["units"] == "degC"

    # Test using input units obtained directly from the converter configuration
    expected = sample_dataset.assign(first=lambda x: x.first - 273.15)  # type: ignore
    converter = UnitsConverter(input_units="degK")
    dataset = converter.convert(sample_dataset, dataset_config, "first")
    assert_close(dataset, expected, check_attrs=False)
    assert dataset["first"].attrs["units"] == "degC"

    # Test case where input units are the same as the output units
    converter = UnitsConverter(input_units="degC")
    dataset = converter.convert(sample_dataset, dataset_config, "first")
    xr.testing.assert_identical(dataset, sample_dataset)  # type: ignore


def test_stringtime_converter(
    sample_dataset: xr.Dataset, dataset_config: DatasetConfig
):
    expected = xr.Dataset(
        coords={
            "time": (
                pd.date_range(  # type: ignore
                    "2022-04-13 14:10:00",
                    "2022-04-13 14:30:00",
                    periods=3,
                )
            )
        },
        data_vars={
            "first": ("time", [59, 60, 61], {"units": "degF"}),
        },
    )
    converter = StringToDatetime(format="%Y-%m-%d %H:%M:%S")
    dataset = converter.convert(sample_dataset, dataset_config, "time")
    assert_close(dataset, expected)

    # TEST: with no time format
    # TEST: with non-UTC timezone
