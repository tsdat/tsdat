import os
import shutil
import tempfile
import xarray as xr
import pandas as pd
from pathlib import Path
from pytest import fixture
from datetime import datetime
from tsdat.io.handlers import NetCDFHandler, ZarrHandler
from tsdat.io.storage import FileSystem, ZarrLocalStorage
from tsdat.testing import assert_close


@fixture
def sample_dataset() -> xr.Dataset:
    time_data = pd.date_range("2022-04-05", "2022-04-06", periods=3)  # type: ignore
    dataset = xr.Dataset(
        coords={"time": time_data},
        data_vars={
            "temperature": (
                "time",
                [
                    71.4,
                    71.2,
                    71.1,
                ],
            ),
        },
        attrs={"datastream": "sgp.testing-storage.a0"},
    )
    return dataset


@fixture
def file_storage():
    storage_root = Path.cwd() / "test/storage_root"
    storage = FileSystem(
        parameters={"storage_root": storage_root},  # type: ignore
        handler=NetCDFHandler(),
    )
    try:
        yield storage
    finally:
        shutil.rmtree(storage.parameters.storage_root)


@fixture
def zarr_storage():
    storage_root = Path.cwd() / "test/storage_root"
    storage = ZarrLocalStorage(
        parameters={"storage_root": storage_root},  # type: ignore
        handler=ZarrHandler(),
    )
    try:
        yield storage
    finally:
        shutil.rmtree(storage.parameters.storage_root)


def test_filesystem_save_and_fetch_data(
    file_storage: FileSystem, sample_dataset: xr.Dataset
):
    expected = sample_dataset.copy(deep=True)  # type: ignore

    # Save
    file_storage.save_data(sample_dataset)
    expected_file = Path(
        file_storage.parameters.storage_root
        / "data"
        / "sgp.testing-storage.a0"
        / "sgp.testing-storage.a0.20220405.000000.nc"
    )
    assert expected_file.is_file()

    # Fetch
    dataset = file_storage.fetch_data(
        start=datetime.fromisoformat("2022-04-05 00:00:00"),
        end=datetime.fromisoformat("2022-04-06 00:00:00"),
        datastream="sgp.testing-storage.a0",
    )
    assert_close(dataset, expected)


def test_zarr_storage_save_and_fetch_data(
    zarr_storage: ZarrLocalStorage, sample_dataset: xr.Dataset
):
    expected = sample_dataset.copy(deep=True)  # type: ignore

    # Save
    zarr_storage.save_data(sample_dataset)
    expected_path = Path(
        zarr_storage.parameters.storage_root / "data" / "sgp.testing-storage.a0.zarr"
    )
    assert expected_path.is_dir()

    # Fetch
    dataset = zarr_storage.fetch_data(
        start=datetime.fromisoformat("2022-04-05 00:00:00"),
        end=datetime.fromisoformat("2022-04-06 00:00:00"),
        datastream="sgp.testing-storage.a0",
    )
    assert_close(dataset, expected)


def test_filesystem_saves_ancillary_files(file_storage: FileSystem):
    expected_filepath = (
        file_storage.parameters.storage_root
        / "ancillary"
        / "sgp.testing-storage.a0"
        / "ancillary_file.txt"
    )

    # Upload directly
    tmp_dir = tempfile.TemporaryDirectory()
    ancillary_filepath = Path(tmp_dir.name) / "ancillary_file.txt"
    ancillary_filepath.write_text("foobar")
    file_storage.save_ancillary_file(
        filepath=ancillary_filepath, datastream="sgp.testing-storage.a0"
    )
    assert expected_filepath.is_file()
    os.remove(expected_filepath)

    # Using context manager
    with file_storage.uploadable_dir(datastream="sgp.testing-storage.a0") as tmp_dir:
        ancillary_filepath = tmp_dir / "ancillary_file.txt"
        ancillary_filepath.write_text("foobar")
    assert expected_filepath.is_file()
    os.remove(expected_filepath)


def test_zarr_storage_saves_ancillary_files(zarr_storage: ZarrLocalStorage):
    expected_filepath = (
        zarr_storage.parameters.storage_root
        / "ancillary"
        / "sgp.testing-storage.a0"
        / "ancillary_file.txt"
    )

    # Upload directly
    tmp_dir = tempfile.TemporaryDirectory()
    ancillary_filepath = Path(tmp_dir.name) / "ancillary_file.txt"
    ancillary_filepath.write_text("foobar")
    zarr_storage.save_ancillary_file(
        filepath=ancillary_filepath, datastream="sgp.testing-storage.a0"
    )
    assert expected_filepath.is_file()
    os.remove(expected_filepath)

    # Using context manager
    with zarr_storage.uploadable_dir(datastream="sgp.testing-storage.a0") as tmp_dir:
        ancillary_filepath = tmp_dir / "ancillary_file.txt"
        ancillary_filepath.write_text("foobar")
    assert expected_filepath.is_file()
    os.remove(expected_filepath)
