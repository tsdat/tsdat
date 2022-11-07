import tempfile
import os
from typing import Any, Dict, Type
import pandas as pd
import xarray as xr
from datetime import datetime
from pathlib import Path
import pytest
from pandas.testing import assert_frame_equal
from tsdat.io.base import DataReader, FileHandler
from tsdat.testing import assert_close
from tsdat.config.utils import recursive_instantiate
from tsdat.io.handlers import (
    CSVHandler,
    NetCDFHandler,
    ParquetHandler,
    ZarrHandler,
)
from tsdat.io.readers import (
    CSVReader,
    NetCDFReader,
    ParquetReader,
    ZarrReader,
    TarReader,
    ZipReader,
)
from tsdat.io.writers import (
    CSVWriter,
    NetCDFWriter,
    SplitNetCDFWriter,
    ParquetWriter,
    ZarrWriter,
)


@pytest.fixture
def sample_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={"index": [0, 1, 2]},
        data_vars={
            "timestamp": (
                "index",
                ["2022-03-24 21:43:00", "2022-03-24 21:44:00", "2022-03-24 21:45:00"],
            ),
            "First Data Var": (
                "index",
                [71.4, 71.2, 71.1],
                {"_FillValue": -9999},
            ),
        },
    )


@pytest.fixture
def sample_dataset_w_time(sample_dataset: xr.Dataset) -> xr.Dataset:
    time_coord = [
        datetime.strptime(x, "%Y-%m-%d %H:%M:%S")
        for x in sample_dataset["timestamp"].data
    ]
    ds = sample_dataset.assign_coords({"index": time_coord}).rename({"index": "time"})  # type: ignore
    ds.attrs["datastream"] = "test_writer"

    return ds


@pytest.fixture
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


@pytest.mark.parametrize(
    "reader_class, input_key",
    [
        (NetCDFReader, "test/io/data/input.nc"),
        (CSVReader, "test/io/data/input.csv"),
        (ParquetReader, "test/io/data/input.parquet"),
        (ZarrReader, "test/io/data/input.zarr"),
    ],
)
def test_file_readers(
    reader_class: Type[DataReader],
    input_key: str,
    sample_dataset: xr.Dataset,
):
    reader = reader_class()
    dataset = reader.read(input_key=input_key)
    assert isinstance(dataset, xr.Dataset)
    assert_close(dataset, sample_dataset, check_fill_value=False)


def test_tar_reader(sample_dataset: xr.Dataset):
    params = {
        "read_tar_kwargs": {"mode": "r:gz"},
        "readers": {
            r".*\.nc": {
                "classname": "tsdat.io.readers.NetCDFReader",
                "parameters": {
                    "engine": "h5netcdf",
                },
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


def test_split_netcdf_writer(sample_dataset_w_time: xr.Dataset):
    params = {"time_interval": 1, "time_unit": "m"}
    writer = SplitNetCDFWriter(parameters=recursive_instantiate(params))
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.nc"
    writer.write(sample_dataset_w_time, tmp_file)  # type: ignore

    filelist = os.listdir(Path(tmp_dir.name))
    filelist.sort()
    assert filelist == [
        "test_writer.20220324.214300.nc",
        "test_writer.20220324.214400.nc",
    ]

    tmp_dir.cleanup()


def test_csv_writer(sample_dataset: xr.Dataset, sample_dataframe: pd.DataFrame):
    expected = sample_dataframe.copy(deep=True)
    writer = CSVWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.csv"
    writer.write(sample_dataset, tmp_file)
    df: pd.DataFrame = pd.read_csv(tmp_file.with_name("test_writer.1D.csv"))  # type: ignore
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


@pytest.mark.parametrize(
    "handler_class, output_key",
    [
        (NetCDFHandler, "test_dataset.nc"),
        (CSVHandler, "test_dataframe.csv"),
        (ParquetHandler, "test_dataframe.parquet"),
        (ZarrHandler, "test_dataset.zarr"),
    ],
)
def test_file_handlers(
    handler_class: Type[FileHandler],
    output_key: str,
    sample_dataset: xr.Dataset,
):
    handler = handler_class()  # type: ignore
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore

    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / output_key
    handler.writer.write(sample_dataset, tmp_file)
    dataset = handler.reader.read(tmp_file.as_posix())
    assert isinstance(dataset, xr.Dataset)
    assert_close(dataset, expected, check_fill_value=False)

    tmp_dir.cleanup()
