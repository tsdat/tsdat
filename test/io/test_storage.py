import os
import shutil
import tempfile
import xarray as xr
import pandas as pd
from pathlib import Path
from pytest import fixture
from datetime import datetime
from tsdat.io.handlers import NetCDFHandler, ZarrHandler
from tsdat.io.storage import FileSystem, ZarrLocalStorage, S3Storage
from tsdat.testing import assert_close
import boto3


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


@fixture
def s3_storage():
    bucket: str = os.environ["STORAGE_BUCKET"]  # TODO: might change to "TSDAT_S3_STORAGE_BUCKET"
    region: str = "us-west-2"  # hard coded for now. region is not required for s3
    storage_root_pre: str = ""  # used to be Path.cwd()
    storage_root = storage_root_pre + "test/storage_root"
    storage = S3Storage(
        parameters={"storage_root": storage_root,
                    "bucket": bucket,
                    "region": region
                    },  # type: ignore
        handler=NetCDFHandler(),

    )
    # print("storage_root: ", storage_root)
    yield storage
    # clean up: delete the test datasets
    s3 = boto3.resource('s3')
    test_bucket = s3.Bucket(bucket)
    for obj in test_bucket.objects.filter(Prefix=storage_root):
        obj.delete()


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


def test_filesystem_save_and_fetch_data_s3(
    s3_storage: S3Storage, sample_dataset: xr.Dataset
):

    expected = sample_dataset.copy(deep=True)  # type: ignore

    # Save/upload to s3
    # s3_storage.save_data_s3(sample_dataset)
    s3_storage.save_data(sample_dataset)
    expected_file_path_local = Path(
        s3_storage.parameters.storage_root
        / "data"
        / "sgp.testing-storage.a0"
        / "sgp.testing-storage.a0.20220405.000000.nc"
    )
    expected_file_path_s3 = str(expected_file_path_local)
    assert s3_storage._is_file_exist_s3(key_name=expected_file_path_s3)

    # Fetch
    # dataset = s3_storage.fetch_data_s3(
    #     start=datetime.fromisoformat("2022-04-05 00:00:00"),
    #     end=datetime.fromisoformat("2022-04-06 00:00:00"),
    #     datastream="sgp.testing-storage.a0",
    # )
    dataset = s3_storage.fetch_data(
        start=datetime.fromisoformat("2022-04-05 00:00:00"),
        end=datetime.fromisoformat("2022-04-06 00:00:00"),
        datastream="sgp.testing-storage.a0",
    )
    assert_close(dataset, expected, check_fill_value=False)  # check_fill_value=False to avoid NAN != None when fillna


def test_filesystem_saves_ancillary_files_s3(s3_storage: S3Storage):
    expected_filepath = str(
        s3_storage.parameters.storage_root
        / "ancillary"
        / "sgp.testing-storage.a0"
        / "ancillary_file.txt"
    )

    # Create a temp file at `ancillary_filepath_src` as resource ancillary file

    # # workflow no.1 temp file at path on S3
    # tmp_dir = tempfile.TemporaryDirectory()
    # ancillary_filepath_src = str(Path(tmp_dir.name) / "ancillary_file.txt")
    # object_bytes = "foobar".encode('utf-8')
    # s3_storage._put_object_s3(object_bytes=object_bytes, file_name_on_s3=ancillary_filepath_src)
    #
    # # Core test
    # s3_storage.save_ancillary_file_s3(
    #     path_src=ancillary_filepath_src, datastream="sgp.testing-storage.a0"
    # )

    # clean up tmp file
    # s3_storage._delete_all_objects_under_prefix(prefix=ancillary_filepath_src)

    # workflow no.2 temp file at path on local
    tmp_dir = tempfile.TemporaryDirectory()
    ancillary_filepath_src = Path(tmp_dir.name) / "ancillary_file.txt"
    ancillary_filepath_src.write_text("foobar")

    # Core test
    s3_storage.save_ancillary_file(
        filepath=ancillary_filepath_src, datastream="sgp.testing-storage.a0"
    )
    assert s3_storage._is_file_exist_s3(key_name=expected_filepath)

    # # clean up tmp file
    # s3_storage._delete_all_objects_under_prefix(prefix=ancillary_filepath_src)
