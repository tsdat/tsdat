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
def sample_2D_dataset() -> xr.Dataset:
    return xr.Dataset(
        coords={"time": [0, 1, 2], "height": [1, 10, 100], "depth": [-1, -2, -4]},
        data_vars={
            "timestamp": (
                "time",
                ["2022-03-24 21:43:00", "2022-03-24 21:44:00", "2022-03-24 21:45:00"],
            ),
            "First Data Var": (
                "time",
                [71.4, 71.2, 71.1],
                {"_FillValue": -9999},
            ),
            "Second Data Var": (
                ["time", "height"],
                [[87.8, 71.1, 2.1], [85.4, 72.2, 5.4], [81.5, 65.3, 4.4]],
                {"_FillValue": -9999},
            ),
            "Third Data Var": (
                ["time", "depth"],
                [[12.7, 18.6, 41.2], [8.3, 15.9, 38.5], [9.7, 17.7, 39.1]],
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
    writer = NetCDFWriter(file_extension=".nc")
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.nc"
    writer.write(sample_dataset, tmp_file)
    dataset: xr.Dataset = xr.open_dataset(tmp_file)  # type: ignore
    assert_close(dataset, expected, check_fill_value=False)

    tmp_dir.cleanup()


def test_netcdf_writer_2D(sample_2D_dataset: xr.Dataset):
    expected = sample_2D_dataset.copy(deep=True)
    writer = NetCDFWriter()
    tmp_dir = tempfile.TemporaryDirectory()
    tmp_file = Path(tmp_dir.name) / "test_writer_2D.nc"
    sample_2D_dataset.encoding["unlimited_dims"] = {"height"}
    writer.write(sample_2D_dataset, tmp_file)
    dataset: xr.Dataset = xr.open_dataset(tmp_file)

    assert "time" in dataset.encoding["unlimited_dims"]
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


def test_csv_writer(sample_2D_dataset: xr.Dataset):
    expected = sample_2D_dataset.to_dataframe()
    writer = CSVWriter()
    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / "test_writer.csv"
    writer.write(sample_2D_dataset, tmp_file)
    df1: pd.DataFrame = pd.read_csv(  # type: ignore
        tmp_file.with_suffix(".time.1d.csv"),
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True,
    )
    df2: pd.DataFrame = pd.read_csv(  # type: ignore
        tmp_file.with_suffix(".height.2d.csv"),
        index_col=[0, 1],
        header=0,
        parse_dates=True,
        infer_datetime_format=True,
    )
    df3: pd.DataFrame = pd.read_csv(  # type: ignore
        tmp_file.with_suffix(".depth.2d.csv"),
        index_col=[0, 1],
        header=0,
        parse_dates=True,
        infer_datetime_format=True,
    )
    df4: pd.DataFrame = df1.join(df2).join(df3)  # type: ignore
    assert_frame_equal(df4, expected)

    tmp_dir.cleanup()


def test_parquet_writer(sample_dataset: xr.Dataset):
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


def test_csv_file_handler(sample_dataset: xr.Dataset):
    handler = CSVHandler()  # type: ignore
    output_key = "test_dataframe.csv"
    expected: xr.Dataset = sample_dataset.copy(deep=True)  # type: ignore

    tmp_dir = tempfile.TemporaryDirectory()

    tmp_file = Path(tmp_dir.name) / output_key
    handler.writer.write(sample_dataset, tmp_file)
    dataset = handler.reader.read(tmp_file.with_suffix(".time.1d.csv").as_posix())
    assert isinstance(dataset, xr.Dataset)
    assert_close(dataset, expected, check_fill_value=False)

    tmp_dir.cleanup()


@pytest.mark.parametrize(
    "handler_class, read_params, write_params",
    [
        (NetCDFHandler, {"engine": "netcdf"}, {"compression_level": 3}),
        (ParquetHandler, {"read_parquet_kwargs": {"engine": "pyarrow"}}, {}),
        (ZarrHandler, {"open_zarr_kwargs": {"decode_times": False}}, {}),
        (CSVHandler, {"read_csv_kwargs": {"delimiter": "\t"}}, {}),
    ],
)
def test_handler_passes_parameters_to_children(
    handler_class: Type[FileHandler],
    read_params: Dict[str, Any],
    write_params: Dict[str, Any],
):
    handler = handler_class(
        parameters=dict(
            reader=read_params,
            writer=write_params,
        )
    )

    # Assert provided read/write params are subset of final parameters
    assert read_params.items() <= dict(handler.reader.parameters).items()
    assert write_params.items() <= dict(handler.writer.parameters).items()


def test_handler_validators():
    handler = NetCDFHandler(
        extension=".nc",
        parameters={
            "reader": {"engine": "netcdf"},
            "writer": {"compression_level": 4},
        },
    )

    assert handler.extension == "nc"
    assert handler.reader.parameters == {"engine": "netcdf"}
    assert handler.writer.parameters.compression_level == 4
