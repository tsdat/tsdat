import tempfile
from typing import Any, Dict
import pandas as pd
import xarray as xr
from pathlib import Path
from pytest import fixture
from pandas.testing import assert_frame_equal
from tsdat.testing import assert_close
from tsdat.config.utils import recursive_instantiate
from tsdat.io.handlers import CSVHandler, NetCDFHandler, ParquetHandler, ZarrHandler
from tsdat.io.readers import (
    CSVReader,
    NetCDFReader,
    ParquetReader,
    ZarrReader,
    TarReader,
    ZipReader,
)
from tsdat.io.writers import CSVWriter, NetCDFWriter, ParquetWriter, ZarrWriter


@fixture
def sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={"index": [0, 1, 2]},
        data_vars={
            "timestamp": (
                "index",
                ["2022-03-24 21:43:00", "2022-03-24 21:44:00", "2022-03-24 21:45:00"],
            ),
            "First Data Var": ("index", [71.4, 71.2, 71.1], {"_FillValue": -9999},),
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


def test_netcdf_reader(sample_dataset: xr.Dataset):
    expected = sample_dataset
    reader = NetCDFReader()
    dataset = reader.read("test/io/data/input.nc")
    assert_close(dataset, expected, check_fill_value=False)


def test_csv_reader(sample_dataset: xr.Dataset):
    expected = sample_dataset
    reader = CSVReader()
    dataset = reader.read("test/io/data/input.csv")
    assert_close(dataset, expected, check_fill_value=False)


def test_parquet_reader(sample_dataset: xr.Dataset):
    expected = sample_dataset
    reader = ParquetReader()
    dataset = reader.read("test/io/data/input.parquet")
    assert_close(dataset, expected, check_fill_value=False)


def test_zarr_reader(sample_dataset: xr.Dataset):
    expected = sample_dataset
    reader = ZarrReader()
    dataset = reader.read("test/io/data/input.zarr")
    assert_close(dataset, expected, check_fill_value=False)


def test_tar_reader(sample_dataset: xr.Dataset):
    params = {
        "read_tar_kwargs": {"mode": "r:gz"},
        "readers": {
            r".*\.nc": {
                "classname": "tsdat.io.readers.NetCDFReader",
                "parameters": {"engine": "h5netcdf",},
            }
        },
    }

    expected = sample_dataset
    reader = TarReader(parameters=recursive_instantiate(params))
    dataset = reader.read("test/io/data/input.tar.gz")
    assert_close(dataset["input.nc"], expected, check_fill_value=False)


def test_zip_reader(sample_dataset: xr.Dataset):
    params = {"readers": {r".*\.nc": {"classname": "tsdat.io.readers.NetCDFReader"}}}

    expected = sample_dataset
    reader = ZipReader(parameters=recursive_instantiate(params))
    dataset = reader.read("test/io/data/input.zip")
    assert_close(dataset["input.nc"], expected, check_fill_value=False)


def test_netcdf_writer(sample_dataset: xr.Dataset):
    expected = sample_dataset.copy(deep=True)  # type: ignore
    writer = NetCDFWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.nc"
    writer.write(sample_dataset, tmp_file)
    dataset: xr.Dataset = xr.open_dataset(tmp_file)  # type: ignore
    assert_close(dataset, expected, check_fill_value=False)

    tmp_dir.cleanup()


def test_csv_writer(sample_dataset: xr.Dataset, sample_dataframe: pd.DataFrame):
    expected = sample_dataframe.copy(deep=True)
    writer = CSVWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.csv"
    writer.write(sample_dataset, tmp_file)
    df: pd.DataFrame = pd.read_csv(tmp_file)  # type: ignore
    assert_frame_equal(df, expected)

    tmp_dir.cleanup()


def test_parquet_writer(sample_dataset: xr.Dataset, sample_dataframe: pd.DataFrame):
    expected = sample_dataset.to_dataframe()
    writer = ParquetWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.parquet"
    writer.write(sample_dataset, tmp_file)
    df: pd.DataFrame = pd.read_parquet(tmp_file)  # type: ignore
    assert_frame_equal(df, expected)

    tmp_dir.cleanup()


def test_zarr_writer(sample_dataset: xr.Dataset):
    expected = sample_dataset.copy(deep=True)  # type: ignore
    writer = ZarrWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.zarr"
    writer.write(sample_dataset, tmp_file)
    dataset: xr.Dataset = xr.open_zarr(tmp_file)  # type: ignore
    assert_close(dataset, expected, check_fill_value=False)

    tmp_dir.cleanup()


def test_netcdf_handler(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    handler = NetCDFHandler()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_dataset.nc"
    handler.writer.write(sample_dataset, tmp_file)
    dataset = handler.reader.read(tmp_file.as_posix())
    assert_close(dataset, expected, check_fill_value=False)

    tmp_dir.cleanup()


def test_csv_handler(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    handler = CSVHandler()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_dataframe.csv"
    handler.writer.write(sample_dataset, tmp_file)
    dataset = handler.reader.read(tmp_file.as_posix())
    assert_close(dataset, expected, check_attrs=False)

    tmp_dir.cleanup()


def test_parquet_handler(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    handler = ParquetHandler()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_dataframe.parquet"
    handler.writer.write(sample_dataset, tmp_file)
    dataset = handler.reader.read(tmp_file.as_posix())
    assert_close(dataset, expected, check_attrs=False)

    tmp_dir.cleanup()


def test_zarr_handler(sample_dataset: xr.Dataset):
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore
    handler = ZarrHandler()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_dataset.zarr"
    handler.writer.write(sample_dataset, tmp_file)
    dataset = handler.reader.read(tmp_file.as_posix())
    assert_close(dataset, expected, check_fill_value=False)

    tmp_dir.cleanup()
