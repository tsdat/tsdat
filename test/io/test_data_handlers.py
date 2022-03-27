import tempfile
from typing import Any, Dict
import pandas as pd
import xarray as xr
from pathlib import Path
from pytest import fixture
from pandas.testing import assert_frame_equal
from test.utils import assert_close
from tsdat.config.dataset import DatasetConfig
from tsdat.io.readers import CSVReader, NetCDFReader
from tsdat.io.writers import CSVWriter, NetCDFWriter


@fixture
def dataset_config():
    return DatasetConfig.from_yaml(Path("test/config/yaml/dataset.yaml"))


@fixture
def sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={"index": [0, 1, 2]},
        data_vars={
            "timestamp": (
                "index",
                ["2022-03-24 21:43:00", "2022-03-24 21:44:00", "2022-03-24 21:45:00"],
            ),
            "First Data Var": ("index", [71.4, 71.2, 71.1]),
        },
    )


@fixture
def sample_dataframe() -> pd.DataFrame:
    data: Dict[str, Any] = {
        "index": [0, 1, 2],
        "timestamp": [
            "2022-03-24 21:43:00",
            "2022-03-24 21:44:00",
            "2022-03-24 21:45:00",
        ],
        "First Data Var": [71.4, 71.2, 71.1],
    }
    return pd.DataFrame(data=data)


def test_netcdf_reader(sample_dataset: xr.Dataset, dataset_config: DatasetConfig):
    expected = sample_dataset
    reader = NetCDFReader()
    dataset = reader.read("test/io/data/input.nc", dataset_config)
    assert_close(dataset, expected)


def test_csv_reader(sample_dataset: xr.Dataset, dataset_config: DatasetConfig):
    expected = sample_dataset
    reader = CSVReader()
    dataset = reader.read("test/io/data/input.csv", dataset_config)
    assert_close(dataset, expected)


def test_netcdf_writer(sample_dataset: xr.Dataset):
    expected = sample_dataset.copy(deep=True)  # type: ignore
    writer = NetCDFWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.nc"
    writer.write(sample_dataset, tmp_file)
    dataset: xr.Dataset = xr.open_dataset(tmp_file)  # type: ignore
    assert_close(dataset, expected)

    tmp_dir.cleanup()


def test_csv_writer(sample_dataset: xr.Dataset, sample_dataframe: pd.DataFrame):
    expected = sample_dataframe
    writer = CSVWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.csv"
    writer.write(sample_dataset, tmp_file)
    df: pd.DataFrame = pd.read_csv(tmp_file)  # type: ignore
    assert_frame_equal(df, expected)

    tmp_dir.cleanup()
