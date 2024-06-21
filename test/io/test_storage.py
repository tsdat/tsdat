import logging
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import moto
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from pytest import fixture

from tsdat.io.base import Storage
from tsdat.io.storage import FileSystem, FileSystemS3, ZarrLocalStorage
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
            data_storage_path="data",
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
    "storage_fixture",
    ["file_storage", "file_storage_v2", "zarr_storage", "s3_storage"],
)
def test_fetch_returns_empty(
    storage_fixture: str,
    request: pytest.FixtureRequest,
):
    # pytest can't pass fixtures through pytest.mark.parametrize so we use this approach
    storage: FileSystem | FileSystemS3 | ZarrLocalStorage = request.getfixturevalue(
        storage_fixture
    )
    storage.parameters.data_storage_path /= "{year}/{month}/{day}"

    expected_dataset = xr.Dataset()  # empty
    dataset = storage.fetch_data(
        start=datetime.fromisoformat("2022-04-10 00:00:00"),
        end=datetime.fromisoformat("2022-04-11 00:00:00"),
        datastream="sgp.testing-storage.a0",
        metadata_kwargs=dict(location_id="sgp"),
    )
    assert_close(dataset, expected_dataset)


@pytest.mark.parametrize(
    "storage_fixture",
    ["file_storage", "s3_storage"],
)
def test_last_modified(
    storage_fixture: str, request: pytest.FixtureRequest, sample_dataset: xr.Dataset
):
    storage: FileSystem | FileSystemS3 = request.getfixturevalue(storage_fixture)

    # Last modified should be robust with regards to the storage path
    storage.parameters.data_storage_path /= "{year}/{month}/{day}/{ext}"

    # Should be empty at first
    datastream = sample_dataset.attrs["datastream"]
    assert storage.last_modified(datastream=datastream) is None

    # Last mod time is date saved, today
    storage.save_data(sample_dataset)
    last_mod = storage.last_modified(datastream=datastream)
    assert last_mod is not None
    assert last_mod.date() == datetime.today().astimezone(timezone.utc).date()

    # Modded files should be empty relative to last saved file
    modded_files = storage.modified_since(datastream=datastream, last_modified=last_mod)
    assert modded_files == []

    # Modded files should be the datetimes of the files saved since the last mod time
    time.sleep(2)  # prevents race condition where files have the same last mod time
    ds1 = sample_dataset.copy(deep=True)
    ds1["time"] = ds1["time"] + np.timedelta64(2, "D")
    storage.save_data(ds1)
    modded_files = storage.modified_since(datastream=datastream, last_modified=last_mod)
    expected_time = pd.to_datetime(ds1["time"].data[0]).to_pydatetime()
    assert modded_files == [expected_time]


@pytest.mark.parametrize(
    "storage_fixture",
    ["file_storage", "s3_storage", "zarr_storage"],
)
def test_filesystem_date_filter(
    storage_fixture: str,
    request: pytest.FixtureRequest,
):
    # pytest can't pass fixtures through pytest.mark.parametrize so we use this approach
    storage: FileSystem = request.getfixturevalue(storage_fixture)

    start = datetime(year=1000, month=1, day=1)
    end = datetime(year=3000, month=1, day=1)

    has_date = [
        Path("data/datastream/sample.file.a0.20230801.000000.nc"),
        Path("data/datastream/sample.file.a0.20230802.000000.nc"),
    ]
    no_date = [Path("data/datastream/sample.file.a0.nc")]

    assert storage._filter_between_dates(has_date, start, end) == has_date

    # Zarr storage doesn't look at dates, others do and filter based on filename
    if storage_fixture == "zarr_storage":
        assert storage._filter_between_dates(no_date, start, end) == no_date
    else:
        assert storage._filter_between_dates(no_date, start, end) == []


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

    # Normal method: use datastream and start time
    with storage.uploadable_dir() as tmp_dir:
        fpath = storage.get_ancillary_filepath(
            title="ancillary",
            extension="png",
            datastream=dataset.attrs["datastream"],
            start=dataset["time"].data[0],
            root_dir=tmp_dir,
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

    # New method: extract needed info from the dataset object
    with storage.uploadable_dir() as tmp_dir:
        fpath = storage.get_ancillary_filepath(
            title="ancillary",
            extension="png",
            dataset=dataset,
            root_dir=tmp_dir,
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


def test_last_modified_zarr(
    zarr_storage: ZarrLocalStorage,
    sample_dataset: xr.Dataset,
    caplog: pytest.LogCaptureFixture,
):
    datastream = sample_dataset.attrs["datastream"]

    with caplog.at_level(logging.WARNING):
        assert zarr_storage.last_modified(datastream) is None
        assert caplog.records[0].levelname == "WARNING"
        assert (
            "ZarrLocalStorage does not support last_modified()"
            in caplog.records[0].message
        )
        caplog.clear()

    with caplog.at_level(logging.WARNING):
        assert zarr_storage.modified_since(datastream, datetime(2022, 1, 1)) == []
        assert caplog.records[0].levelname == "WARNING"
        assert (
            "ZarrLocalStorage does not support modified_since()"
            in caplog.records[0].message
        )
