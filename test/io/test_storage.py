import os
import shutil
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
    storage = FileSystem(parameters={"storage_root": Path.cwd() / "test/storage_root"})  # type: ignore
    try:
        yield storage
    finally:
        shutil.rmtree(storage.parameters.storage_root)


@fixture
def zarr_storage():
    storage = ZarrLocalStorage(parameters={"storage_root": Path.cwd() / "test/storage_root"})  # type: ignore
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
        parameters={
            "bucket": "tsdat-core",
            "storage_root": storage_root,
            "region": "us-east-1",
        },  # type: ignore
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

    # main data
    expected_dataset: xr.Dataset = input_dataset.copy(deep=True)  # type: ignore
    storage.save_data(dataset=input_dataset)

    # ancillary files
    with storage.uploadable_dir(dataset=input_dataset) as tmp_dir:
        ancillary_filepath = tmp_dir / "ancillary_file.txt"
        ancillary_filepath.write_text("foobar")
    expected_filepath = (
        storage.parameters.storage_root
        / "sgp.testing-storage.a0"
        / "ancillary"
        / "ancillary_file.txt"
    )

    # fetch data
    dataset = storage.fetch_data(
        start=datetime.fromisoformat("2022-04-05 00:00:00"),
        end=datetime.fromisoformat("2022-04-06 00:00:00"),
        datastream="sgp.testing-storage.a0",
    )

    assert_close(input_dataset, expected_dataset)  # storage should not modify inputs
    if "file" in storage_fixture or "zarr" in storage_fixture:
        assert expected_filepath.is_file()
        os.remove(expected_filepath)
    elif "s3" in storage_fixture:
        assert storage.exists(expected_filepath)
        obj = storage.get_obj(expected_filepath)
        assert obj is not None
        obj.delete()  # type: ignore
    assert_close(dataset, expected_dataset)
