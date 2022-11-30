import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import moto
import pytest
import pandas as pd
import xarray as xr
from pytest import fixture
from tsdat.io.base import Storage

from tsdat.io.storage import FileSystem, ZarrLocalStorage, FileSystemS3
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
    storage = FileSystem(parameters=FileSystem.Parameters(storage_root=Path.cwd() / "test/storage_root"))  # type: ignore
    try:
        yield storage
    finally:
        shutil.rmtree(storage.parameters.storage_root)


@fixture
def zarr_storage():
    storage = ZarrLocalStorage(
        parameters=ZarrLocalStorage.Parameters(**{"storage_root": Path.cwd() / "test/storage_root"})  # type: ignore
    )
    try:
        yield storage
    finally:
        shutil.rmtree(storage.parameters.storage_root)


@fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@fixture
def s3_storage(aws_credentials: Any):
    s3, sts = moto.mock_s3(), moto.mock_sts()  # type: ignore
    s3.start(), sts.start()  # type: ignore
    storage_root = Path("test/storage_root")
    storage = FileSystemS3(
        parameters=FileSystemS3.Parameters(
            **{
                "bucket": "tsdat-core",
                "storage_root": storage_root,
                "region": "us-east-1",
            },  # type: ignore
        )
    )
    try:
        yield storage
    finally:
        storage.bucket.objects.filter(Prefix=str(storage_root)).delete()
        s3.stop(), sts.stop()  # type: ignore


@pytest.mark.parametrize(
    "storage_fixture, dataset_fixture",
    [
        ("file_storage", "sample_dataset"),
        ("zarr_storage", "sample_dataset"),
        ("s3_storage", "sample_dataset"),
    ],
)
def test_storage_saves_and_fetches_data(
    storage_fixture: str,
    dataset_fixture: str,
    request: pytest.FixtureRequest,
):
    # pytest can't pass fixtures through pytest.mark.parametrize so we use this approach
    storage: Storage = request.getfixturevalue(storage_fixture)
    input_dataset: xr.Dataset = request.getfixturevalue(dataset_fixture)

    expected_dataset: xr.Dataset = input_dataset.copy(deep=True)  # type: ignore
    storage.save_data(dataset=input_dataset)
    dataset = storage.fetch_data(
        start=datetime.fromisoformat("2022-04-05 00:00:00"),
        end=datetime.fromisoformat("2022-04-06 00:00:00"),
        datastream="sgp.testing-storage.a0",
    )
    assert_close(input_dataset, expected_dataset)  # storage should not modify inputs
    assert_close(dataset, expected_dataset)


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


def test_filesystem_s3_saves_ancillary_files(s3_storage: FileSystemS3):
    expected_filepath = (
        s3_storage.parameters.storage_root
        / "ancillary"
        / "sgp.testing-storage.a0"
        / "ancillary_file.txt"
    )

    # Upload directly
    tmp_dir = tempfile.TemporaryDirectory()
    ancillary_filepath = Path(tmp_dir.name) / "ancillary_file.txt"
    ancillary_filepath.write_text("foobar")
    s3_storage.save_ancillary_file(
        filepath=ancillary_filepath, datastream="sgp.testing-storage.a0"
    )
    assert s3_storage.exists(expected_filepath)
    obj = s3_storage.get_obj(expected_filepath)
    assert obj is not None
    obj.delete()

    # Using context manager
    with s3_storage.uploadable_dir(datastream="sgp.testing-storage.a0") as tmp_dir:
        ancillary_filepath = tmp_dir / "ancillary_file.txt"
        ancillary_filepath.write_text("foobar")
    assert s3_storage.exists(expected_filepath)
    obj = s3_storage.get_obj(expected_filepath)
    assert obj is not None
    obj.delete()
