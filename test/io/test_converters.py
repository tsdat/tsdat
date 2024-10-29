from pathlib import Path

import pandas as pd
import xarray as xr
from pytest import fixture

from tsdat import (
    DatasetConfig,
    RetrievedDataset,
    StringToDatetime,
    UnitsConverter,
)


@fixture
def dataset_config() -> DatasetConfig:
    return DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))


def test_units_converter(
    multi_var_1D_dataset: xr.Dataset, dataset_config: DatasetConfig
):
    retrieved_dataset = RetrievedDataset.from_xr_dataset(multi_var_1D_dataset)

    # Test using input units obtained from the 'raw' (sample) dataset
    expected = multi_var_1D_dataset.assign(first=lambda x: (x.first - 32) * 5 / 9)  # type: ignore
    converter = UnitsConverter(input_units=None)
    data = converter.convert(
        multi_var_1D_dataset["first"], "first", dataset_config, retrieved_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["first"])  # type: ignore
    assert data.attrs["units"] == "degC"

    # Test using input units obtained directly from the converter configuration
    expected = multi_var_1D_dataset.assign(first=lambda x: x.first - 273.15)  # type: ignore
    converter = UnitsConverter(input_units="degK")
    data = converter.convert(
        multi_var_1D_dataset["first"], "first", dataset_config, retrieved_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["first"])  # type: ignore
    assert data.attrs["units"] == "degC"

    # Test case where input units are the same as the output units
    converter = UnitsConverter(input_units="degC")
    data = converter.convert(
        multi_var_1D_dataset["first"], "first", dataset_config, retrieved_dataset
    )
    assert data is not None


def test_defined_pint_units(
    multi_var_1D_dataset: xr.Dataset, dataset_config: DatasetConfig
):
    retrieved_dataset = RetrievedDataset.from_xr_dataset(multi_var_1D_dataset)

    # Manually convert fahrenheit to celsius
    expected = multi_var_1D_dataset.assign(temp=lambda x: (x["temp"] - 32) * 5 / 9)  # type: ignore
    # Use units here since dataset.yaml not updated
    converter = UnitsConverter()
    # Sample dataset is the input file, dataset_config shows output
    data = converter.convert(
        multi_var_1D_dataset["temp"], "first", dataset_config, retrieved_dataset
    )

    assert data is not None
    xr.testing.assert_allclose(data, expected["temp"])  # type: ignore
    assert data.attrs["units"] == "degC"

    # Convert dimensionless to percent – no conversion other than proper units
    expected = multi_var_1D_dataset.assign(percent=lambda x: (x["percent"]))  # type: ignore
    converter = UnitsConverter()
    dataset_config["first"].attrs.units = "%"  # Set output units in dataset config
    data = converter.convert(
        multi_var_1D_dataset["percent"], "first", dataset_config, retrieved_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["percent"])  # type: ignore
    assert data.attrs["units"] == "%"

    # Convert km s-1 to m s-1
    expected = multi_var_1D_dataset.assign(exponent=lambda x: (x["exponent"] * 1000))  # type: ignore
    converter = UnitsConverter()
    # Set output units in dataset config
    dataset_config["first"].attrs.units = "m s-1"
    data = converter.convert(
        multi_var_1D_dataset["exponent"], "first", dataset_config, retrieved_dataset
    )

    assert data is not None
    xr.testing.assert_allclose(data, expected["exponent"])  # type: ignore
    assert data.attrs["units"] == "m s-1"


def test_stringtime_converter(
    multi_var_1D_dataset: xr.Dataset, dataset_config: DatasetConfig
):
    expected = multi_var_1D_dataset.copy(deep=True)  # type: ignore
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
    ret_dataset = RetrievedDataset.from_xr_dataset(multi_var_1D_dataset)

    # convert string to datetime with explicit settings
    converter = StringToDatetime(format="%Y-%m-%d %H:%M:%S", timezone="UTC")
    data = converter.convert(
        multi_var_1D_dataset["time"], "time", dataset_config, ret_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["time"])  # type: ignore

    # no time format
    converter = StringToDatetime()
    data = converter.convert(
        multi_var_1D_dataset["time"], "time", dataset_config, ret_dataset
    )
    assert data is not None
    xr.testing.assert_allclose(data, expected["time"])  # type: ignore

    # non-UTC timezone
    converter = StringToDatetime(format="%Y-%m-%d %H:%M:%S", timezone="US/Pacific")
    data = converter.convert(
        multi_var_1D_dataset["time"], "time", dataset_config, ret_dataset
    )
    assert data is not None
    assert (data.data - pd.Timedelta(hours=7) == expected.time.data).all()
