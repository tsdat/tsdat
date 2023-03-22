import pandas as pd
import xarray as xr
from pathlib import Path
from pytest import fixture
from tsdat import (
    DatasetConfig,
    StringToDatetime,
    UnitsConverter,
    RetrievedDataset,
)


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
    retrieved_dataset = RetrievedDataset.from_xr_dataset(sample_dataset)

    # Test using input units obtained from the 'raw' (sample) dataset
    expected = sample_dataset.assign(first=lambda x: (x.first - 32) * 5 / 9)  # type: ignore
    converter = UnitsConverter(input_units=None)
    data = converter.convert(
        sample_dataset["first"], "first", dataset_config, retrieved_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["first"])  # type: ignore
    assert data.attrs["units"] == "degC"

    # Test using input units obtained directly from the converter configuration
    expected = sample_dataset.assign(first=lambda x: x.first - 273.15)  # type: ignore
    converter = UnitsConverter(input_units="degK")
    data = converter.convert(
        sample_dataset["first"], "first", dataset_config, retrieved_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["first"])  # type: ignore
    assert data.attrs["units"] == "degC"

    # Test case where input units are the same as the output units
    converter = UnitsConverter(input_units="degC")
    data = converter.convert(
        sample_dataset["first"], "first", dataset_config, retrieved_dataset
    )
    assert data is None

    # Test case where there are no input units
    converter = UnitsConverter()
    data = converter.convert(
        sample_dataset["second"], "second", dataset_config, retrieved_dataset
    )
    assert data is None


def test_stringtime_converter(
    sample_dataset: xr.Dataset, dataset_config: DatasetConfig
):
    expected = sample_dataset.copy(deep=True)  # type: ignore
    expected["_time"] = xr.DataArray(
        data=pd.date_range(  # type: ignore
            "2022-04-13 14:10:00",
            "2022-04-13 14:30:00",
            periods=3,
        ),
        coords={"time": expected["time"]},
    )
    expected = expected.swap_dims({"time": "_time"})  # type: ignore
    expected = expected.drop_vars(["time"]).rename({"_time": "time"})
    ret_dataset = RetrievedDataset.from_xr_dataset(sample_dataset)

    # convert string to datetime with explicit settings
    converter = StringToDatetime(format="%Y-%m-%d %H:%M:%S", timezone="UTC")
    data = converter.convert(
        sample_dataset["time"], "time", dataset_config, ret_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["time"])  # type: ignore

    # no time format
    converter = StringToDatetime()
    data = converter.convert(
        sample_dataset["time"], "time", dataset_config, ret_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["time"])  # type: ignore

    # non-UTC timezone
    converter = StringToDatetime(format="%Y-%m-%d %H:%M:%S", timezone="US/Pacific")
    data = converter.convert(
        sample_dataset["time"], "time", dataset_config, ret_dataset
    )
    assert data is not None
    assert (data.data - pd.Timedelta(hours=7) == expected.time.data).all()
