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
        attrs={
            "location_id": "sgp",  # used in S3 path substitution
            "datastream": "sgp.testing-storage.a0",
        },
    )
    return dataset


@fixture
def file_storage():
    # Using cwd() results in absolute path (leading "/")
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root=Path.cwd() / "test/storage_root",
            data_storage_path="data/{datastream}",
            ancillary_storage_path="ancillary/{location_id}",
        )
    )
    try:
        yield storage
    finally:
        shutil.rmtree(storage.parameters.storage_root)


@fixture
def file_storage_v2():
    storage = FileSystem(
        parameters=FileSystem.Parameters(
            storage_root="test/storage_root",
            data_storage_path="{location_id}/{year}/{month}/{day}/{datastream}",
            ancillary_storage_path="ancillary/{year}/{month}",
        )  # type: ignore
    )
    try:
        yield storage
    finally:
        shutil.rmtree(storage.parameters.storage_root)


@fixture
def zarr_storage():
    storage = ZarrLocalStorage(
        parameters=ZarrLocalStorage.Parameters(
            storage_root=Path.cwd() / "test/storage_root",
            data_storage_path="data/{datastream}",
            ancillary_storage_path="ancillary/{date_time}",
            ancillary_filename_template="{datastream}.{title}.{extension}",
        )  # type: ignore
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
        storage._bucket.objects.filter(Prefix=str(storage_root)).delete()
        s3.stop(), sts.stop()  # type: ignore


@pytest.mark.parametrize(
    "storage_fixture, dataset_fixture",
    [
        ("file_storage", "sample_dataset"),
        ("file_storage_v2", "sample_dataset"),
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
        metadata_kwargs=dict(location_id="sgp"),
    )
    assert_close(input_dataset, expected_dataset)  # storage should not modify inputs
    assert_close(dataset, expected_dataset)


@pytest.mark.parametrize(
    "storage_fixture, dataset_fixture, expected",
    [
        (
            "file_storage",
            "sample_dataset",
            "ancillary/sgp/sgp.testing-storage.a0.20220405.000000.ancillary.png",
        ),
        (
            "file_storage_v2",
            "sample_dataset",
            "ancillary/2022/04/sgp.testing-storage.a0.20220405.000000.ancillary.png",
        ),
        (
            "zarr_storage",
            "sample_dataset",
            "ancillary/20220405.000000/sgp.testing-storage.a0.ancillary.png",
        ),
        (
            "s3_storage",
            "sample_dataset",
            "ancillary/sgp/sgp.testing-storage.a0/sgp.testing-storage.a0.20220405.000000.ancillary.png",
        ),
    ],
)
def test_storage_saves_ancillary_files(
    storage_fixture: str,
    dataset_fixture: str,
    expected: str,
    request: pytest.FixtureRequest,
):
    # pytest can't pass fixtures through pytest.mark.parametrize so we use this approach
    storage: Storage = request.getfixturevalue(storage_fixture)
    dataset: xr.Dataset = request.getfixturevalue(dataset_fixture)

    expected_filepath = storage.parameters.storage_root / expected

    with storage.uploadable_dir() as tmp_dir:
        fpath = storage.get_ancillary_filepath(
            title="ancillary", extension="png", dataset=dataset, root_dir=tmp_dir
        )
        fpath.touch()
    if storage_fixture == "s3_storage":
        assert storage._exists(expected_filepath)
        obj = storage._get_obj(expected_filepath)
        assert obj is not None
        obj.delete()
    else:
        assert expected_filepath.exists()
        os.remove(expected_filepath)
