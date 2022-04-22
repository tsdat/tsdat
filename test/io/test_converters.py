import pandas as pd
import xarray as xr
from pathlib import Path
from pytest import fixture
from tsdat.config.dataset import DatasetConfig
from tsdat.io.converters import StringToDatetime, UnitsConverter
from tsdat.testing import assert_close


@fixture
def sample_dataset() -> xr.Dataset:
    ds = xr.Dataset(
        coords={
            "time": (
                ["2022-04-13 14:10:00", "2022-04-13 14:20:00", "2022-04-13 14:30:00"]
            )
        },
        data_vars={
            "first": (
                "time",
                [59, 60, 61],
                {"units": "degF"},
            ),
            "second": (
                "time",
                [59, 60, 61],
                {"comment": "test case with no units attr"},
            ),
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
    assert_close(dataset, sample_dataset)

    # Test case where there are no input units
    converter = UnitsConverter()
    dataset = converter.convert(sample_dataset, dataset_config, "second")
    assert_close(dataset, sample_dataset)


def test_stringtime_converter(
    sample_dataset: xr.Dataset, dataset_config: DatasetConfig
):
    expected = sample_dataset.copy(deep=True) # type: ignore
    expected["_time"] = xr.DataArray(
        data=pd.date_range(  # type: ignore
            "2022-04-13 14:10:00",
            "2022-04-13 14:30:00",
            periods=3,
        ),
        coords={"time": expected["time"]},
    )
    expected = expected.swap_dims({"time": "_time"}) # type: ignore
    expected = expected.drop_vars(["time"]).rename({"_time": "time"})
    # expected = xr.Dataset(
    #     coords={
    #         "time": (
    #             "time",
    #             pd.date_range(  # type: ignore
    #                 "2022-04-13 14:10:00",
    #                 "2022-04-13 14:30:00",
    #                 periods=3,
    #             ),
    #             {"units": "Seconds since 1970-01-01 00:00:00"},
    #         )
    #     },
    # )
    # convert string to datetime with explicit settings
    converter = StringToDatetime(format="%Y-%m-%d %H:%M:%S", timezone="UTC")
    dataset = converter.convert(sample_dataset, dataset_config, "time")
    assert_close(dataset, expected)

    # no time format
    converter = StringToDatetime()
    dataset = converter.convert(sample_dataset, dataset_config, "time")
    assert_close(dataset, expected)

    # non-UTC timezone
    converter = StringToDatetime(format="%Y-%m-%d %H:%M:%S", timezone="US/Pacific")
    dataset = converter.convert(sample_dataset, dataset_config, "time")
    assert (dataset.time.data - pd.Timedelta(hours=7) == expected.time.data).all()
