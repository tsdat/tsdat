from functools import lru_cache
import logging
import os
import shutil
from time import time
import xarray as xr
from datetime import datetime
from pydantic import BaseSettings, validator, Field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from .base import Storage
from .handlers import FileHandler, NetCDFHandler, ZarrHandler
from ..utils import get_filename

import boto3
import botocore.exceptions

import tempfile


__all__ = ["FileSystem", "FileSystemS3", "ZarrLocalStorage"]

# IDEA: interval / split files apart by some timeframe (e.g., 1 day)
#
# Optional:
# file_timespan: 1D
#
#
# psuedocode: Solely for splitting up a file into multiple chunks. Searching for
# previous + merging probably happens when you actually store the dataset, if that's
# something we care about

# start_time = 00:00:00 (midnight for the date of the first timestamp in the dataset)
# first_interval = [start_time: start_time + file_time_interval]
# start_time += file_time_interval
# until start_time + file_time_interval >= timestamp of the last point of the dataset
logger = logging.getLogger(__name__)


class FileSystem(Storage):
    """------------------------------------------------------------------------------------
    Handles data storage and retrieval for file-based data formats.

    Formats that write to directories (such as zarr) are not supported by the FileSystem
    storage class.

    Args:
        parameters (Parameters): File-system specific parameters, such as the root path to
            where files should be saved, or additional keyword arguments to specific
            functions used by the storage API. See the FileSystemStorage.Parameters class for
            more details.
        handler (FileHandler): The FileHandler class that should be used to handle data
            I/O within the storage API.

    ------------------------------------------------------------------------------------"""

    # TODO: @clansing refactor to use a 'StorageFile' class for custom file naming
    # conventions. Until then, we will assume that we are using tsdat naming conventions
    # e.g., datastream = location.dataset_name[-qualifier][-temporal].data_level,
    # filename = datastream.YYYYMMDD.hhmmss.<extension>
    # filepath = <storage root>/location/datastream/filename

    class Parameters(BaseSettings):
        storage_root: Path = Path.cwd() / "storage" / "root"
        """The path on disk where data and ancillary files will be saved to. Defaults to
        the `storage/root` folder in the active working directory. The directory is
        created as this parameter is set, if the directory does not already exist."""

        file_timespan: Optional[str] = None
        merge_fetched_data_kwargs: Dict[str, Any] = dict()

        @validator("storage_root")
        def _ensure_storage_root_exists(cls, storage_root: Path) -> Path:
            if not storage_root.is_dir():
                logger.info("Creating storage root at: %s", storage_root.as_posix())
                storage_root.mkdir(parents=True)
            return storage_root

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore
    handler: FileHandler = Field(default_factory=NetCDFHandler)

    def save_data(self, dataset: xr.Dataset):
        """-----------------------------------------------------------------------------
        Saves a dataset to the storage area.

        At a minimum, the dataset must have a 'datastream' global attribute and must
        have a 'time' variable with a np.datetime64-like data type.

        Args:
            dataset (xr.Dataset): The dataset to save.

        -----------------------------------------------------------------------------"""
        datastream = dataset.attrs["datastream"]
        filepath = self._get_dataset_filepath(dataset, datastream)
        filepath.parent.mkdir(exist_ok=True, parents=True)
        self.handler.writer.write(dataset, filepath)
        logger.info("Saved %s dataset to %s", datastream, filepath.as_posix())

    def fetch_data(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Fetches data for a given datastream between a specified time range.

        Note: this method is not smart; it searches for the appropriate data files using
        their filenames and does not filter within each data file.

        Args:
            start (datetime): The minimum datetime to fetch.
            end (datetime): The maximum datetime to fetch.
            datastream (str): The datastream id to search for.

        Returns:
            xr.Dataset: A dataset containing all the data in the storage area that spans
            the specified datetimes.

        -----------------------------------------------------------------------------"""
        data_files = self._find_data(start, end, datastream)
        datasets = self._open_data_files(*data_files)
        return xr.merge(datasets, **self.parameters.merge_fetched_data_kwargs)  # type: ignore

    def save_ancillary_file(self, filepath: Path, datastream: str):
        """-----------------------------------------------------------------------------
        Saves an ancillary filepath to the datastream's ancillary storage area.

        Args:
            filepath (Path): The path to the ancillary file.
            datastream (str): The datastream that the file is related to.

        -----------------------------------------------------------------------------"""
        ancillary_filepath = self._get_ancillary_filepath(filepath, datastream)
        ancillary_filepath.parent.mkdir(exist_ok=True, parents=True)
        saved_filepath = shutil.copy2(filepath, ancillary_filepath)
        logger.info("Saved ancillary file to: %s", saved_filepath)

    def _find_data(self, start: datetime, end: datetime, datastream: str) -> List[Path]:
        data_dirpath = self.parameters.storage_root / "data" / datastream
        filepaths = [data_dirpath / Path(file) for file in os.listdir(data_dirpath)]
        return self._filter_between_dates(filepaths, start, end)

    def _filter_between_dates(
        self, filepaths: List[Path], start: datetime, end: datetime
    ) -> List[Path]:
        # HACK: Currently can overshoot on both sides of the given range because we only
        # use the start date from the filename.
        def __get_date_str(file: Path) -> str:
            name_components = file.name.split(".")
            date_components = name_components[3:5]
            return ".".join(date_components)

        start_date_str = start.strftime("%Y%m%d.%H%M%S")
        end_date_str = end.strftime("%Y%m%d.%H%M%S")

        valid_filepaths: List[Path] = []
        for filepath in filepaths:
            file_date_str = __get_date_str(filepath)
            if start_date_str <= file_date_str <= end_date_str:
                valid_filepaths.append(filepath)
        return valid_filepaths

    def _open_data_files(self, *filepaths: Path) -> List[xr.Dataset]:
        dataset_list: List[xr.Dataset] = []
        for filepath in filepaths:
            data = self.handler.reader.read(filepath.as_posix())
            if isinstance(data, dict):
                data = xr.merge(data.values())  # type: ignore
            dataset_list.append(data)
        return dataset_list

    def _get_dataset_filepath(self, dataset: xr.Dataset, datastream: str) -> Path:
        datastream_dir = self.parameters.storage_root / "data" / datastream
        extension = self.handler.writer.file_extension
        return datastream_dir / get_filename(dataset, extension)

    def _get_ancillary_filepath(self, filepath: Path, datastream: str) -> Path:
        anc_datastream_dir = self.parameters.storage_root / "ancillary" / datastream
        return anc_datastream_dir / filepath.name


class FileSystemS3(FileSystem):
    """------------------------------------------------------------------------------------
    Handles data storage and retrieval for file-based data formats in an AWS S3 bucket.

    Args:
        parameters (Parameters): File-system and AWS-specific parameters, such as the root
            path to where files should be saved, or additional keyword arguments to
            specific functions used by the storage API. See the S3Storage.Parameters
            class for more details.
        handler (FileHandler): The FileHandler class that should be used to handle data
            I/O within the storage API.

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseSettings):  # type: ignore
        storage_root: Path = Field(Path("storage/root"), env="TSDAT_STORAGE_ROOT")
        """The path on disk where data and ancillary files will be saved to. Defaults to
        the `storage/root` folder in the top level of the storage bucket."""

        bucket: str = Field("tsdat-storage", env="TSDAT_S3_BUCKET_NAME")
        """The name of the S3 bucket that the storage class should attach to."""

        region: str = Field("us-west-2", env="AWS_DEFAULT_REGION")
        """The AWS region of the storage bucket. Defaults to "us-west-2"."""

        merge_fetched_data_kwargs: Dict[str, Any] = dict()
        """Keyword arguments to xr.merge. Note: this will only be called if the
        DataReader returns a dictionary of xr.Datasets for a single saved file."""

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore

    @validator("parameters")
    def check_authentication(cls, parameters: Parameters):
        session = FileSystemS3._get_session(
            region=parameters.region, timehash=FileSystemS3._get_timehash()
        )
        try:
            session.client("sts").get_caller_identity().get("Account")  # type: ignore
        except botocore.exceptions.ClientError:
            raise ValueError(
                "Could not connect to the AWS client. This is likely due to"
                " misconfigured or expired credentials."
            )
        return parameters

    @validator("parameters")
    def ensure_bucket_exists(cls, parameters: Parameters):
        session = FileSystemS3._get_session(
            region=parameters.region, timehash=FileSystemS3._get_timehash()
        )
        s3 = session.resource("s3", region_name=parameters.region)  # type: ignore
        try:
            s3.meta.client.head_bucket(Bucket=parameters.bucket)
        except botocore.exceptions.ClientError:
            logger.warning("Creating bucket '%s'.", parameters.bucket)
            s3.create_bucket(Bucket=parameters.bucket)
        return parameters

    @property
    def session(self):
        return FileSystemS3._get_session(
            region=self.parameters.region, timehash=FileSystemS3._get_timehash()
        )

    @property
    def bucket(self):
        s3 = self.session.resource("s3", region_name=self.parameters.region)  # type: ignore
        return s3.Bucket(name=self.parameters.bucket)

    @staticmethod
    @lru_cache()
    def _get_session(region: str, timehash: int = 0):
        """------------------------------------------------------------------------------------
        Creates a boto3 Session or returns an active one.

        Borrowed approximately from https://stackoverflow.com/a/55900800/15641512.

        Args:
            region (str): The session region.
            timehash (int, optional): A time hash used to cache repeated calls to this
                function. This should be generated using tsdat.io.storage.get_timehash().

        Returns:
            boto3.session.Session: An active boto3 Session object.

        ------------------------------------------------------------------------------------"""
        del timehash
        return boto3.session.Session(region_name=region)

    @staticmethod
    def _get_timehash(seconds: int = 3600) -> int:
        return round(time() / seconds)

    def save_data(self, dataset: xr.Dataset):
        datastream: str = dataset.attrs["datastream"]
        standard_fpath = self._get_dataset_filepath(dataset, datastream)

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_filepath = Path(tmp_dir) / standard_fpath
            tmp_filepath.parent.mkdir(parents=True, exist_ok=True)

            self.handler.writer.write(dataset, tmp_filepath)

            for filepath in Path(tmp_dir).glob("**/*"):
                if filepath.is_dir():
                    continue
                s3_filepath = filepath.relative_to(tmp_dir).as_posix()
                self.bucket.upload_file(Filename=filepath.as_posix(), Key=s3_filepath)
                logger.info(
                    "Saved %s data file to s3://%s/%s",
                    datastream,
                    self.parameters.bucket,
                    s3_filepath,
                )

    def save_ancillary_file(self, filepath: Path, datastream: str):
        s3_filepath = self._get_ancillary_filepath(filepath, datastream)
        self.bucket.upload_file(Filename=str(filepath), Key=str(s3_filepath))
        logger.info("Saved %s ancillary file to: %s", filepath.name, str(s3_filepath))

    def _find_data(self, start: datetime, end: datetime, datastream: str) -> List[Path]:
        prefix = str(self.parameters.storage_root / "data" / datastream) + "/"
        objects = self.bucket.objects.filter(Prefix=prefix)
        filepaths = [
            Path(obj.key) for obj in objects if obj.key.endswith(self.handler.extension)
        ]
        return self._filter_between_dates(filepaths, start, end)

    def _open_data_files(self, *filepaths: Path) -> List[xr.Dataset]:
        dataset_list: List[xr.Dataset] = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            for s3_filepath in filepaths:
                tmp_filepath = str(Path(tmp_dir) / s3_filepath.name)
                self.bucket.download_file(
                    Key=str(s3_filepath),
                    Filename=tmp_filepath,
                )
                data = self.handler.reader.read(tmp_filepath)
                if isinstance(data, dict):
                    data = xr.merge(data.values())  # type: ignore
                data = data.compute()  # type: ignore
                dataset_list.append(data)
        return dataset_list

    def exists(self, key: Union[Path, str]) -> bool:
        return self.get_obj(str(key)) is not None

    def get_obj(self, key: Union[Path, str]):
        objects = self.bucket.objects.filter(Prefix=str(key))
        try:
            return next(obj for obj in objects if obj.key == str(key))
        except StopIteration:
            return None


class ZarrLocalStorage(Storage):
    """---------------------------------------------------------------------------------
    Handles data storage and retrieval for zarr archives on a local filesystem.

    Zarr is a special format that writes chunked data to a number of files underneath
    a given directory. This distribution of data into chunks and distinct files makes
    zarr an extremely well-suited format for quickly storing and retrieving large
    quantities of data.

    Args:
        parameters (Parameters): File-system specific parameters, such as the root path
            to where the Zarr archives should be saved, or additional keyword arguments
            to specific functions used by the storage API. See the Parameters class for
            more details.

        handler (ZarrHandler): The ZarrHandler class that should be used to handle data
            I/O within the storage API.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseSettings):
        storage_root: Path = Path.cwd() / "storage" / "root"
        """The path on disk where data and ancillary files will be saved to. Defaults to
        the `storage/root` folder in the active working directory. The directory is
        created as this parameter is set, if the directory does not already exist."""

    parameters: Parameters = Field(default_factory=Parameters)
    handler: ZarrHandler = Field(default_factory=ZarrHandler)

    def save_data(self, dataset: xr.Dataset):
        """-----------------------------------------------------------------------------
        Saves a dataset to the storage area.

        At a minimum, the dataset must have a 'datastream' global attribute and must
        have a 'time' variable with a np.datetime64-like data type.

        Args:
            dataset (xr.Dataset): The dataset to save.

        -----------------------------------------------------------------------------"""
        datastream = dataset.attrs["datastream"]
        dataset_path = self._get_dataset_path(datastream)
        dataset_path.mkdir(exist_ok=True, parents=True)
        self.handler.writer.write(dataset, dataset_path)
        logger.info("Saved %s dataset to %s", datastream, dataset_path.as_posix())

    def fetch_data(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Fetches data for a given datastream between a specified time range.

        Args:
            start (datetime): The minimum datetime to fetch (inclusive).
            end (datetime): The maximum datetime to fetch (exclusive).
            datastream (str): The datastream id to search for.

        Returns:
            xr.Dataset: A dataset containing all the data in the storage area that spans
            the specified datetimes.

        -----------------------------------------------------------------------------"""
        datastream_path = self._get_dataset_path(datastream)
        full_dataset = self.handler.reader.read(datastream_path.as_posix())
        dataset_in_range = full_dataset.sel(time=slice(start, end))
        return dataset_in_range.compute()  # type: ignore

    def save_ancillary_file(self, filepath: Path, datastream: str):
        """-----------------------------------------------------------------------------
        Saves an ancillary filepath to the datastream's ancillary storage area.

        Args:
            filepath (Path): The path to the ancillary file.
            datastream (str): The datastream that the file is related to.

        -----------------------------------------------------------------------------"""
        ancillary_filepath = self._get_ancillary_filepath(filepath, datastream)
        ancillary_filepath.parent.mkdir(exist_ok=True, parents=True)
        saved_filepath = shutil.copy2(filepath, ancillary_filepath)
        logger.info("Saved ancillary file to: %s", saved_filepath)

    def _get_dataset_path(self, datastream: str) -> Path:
        datastream_dir = self.parameters.storage_root / "data" / datastream
        extension = self.handler.writer.file_extension
        return datastream_dir.parent / (datastream_dir.name + extension)

    def _get_ancillary_filepath(self, filepath: Path, datastream: str) -> Path:
        anc_datastream_dir = self.parameters.storage_root / "ancillary" / datastream
        return anc_datastream_dir / filepath.name


# HACK: Update forward refs to get around error I couldn't replicate with simpler code
# "pydantic.errors.ConfigError: field "parameters" not yet prepared so type is still a ForwardRef..."
FileSystem.update_forward_refs(Parameters=FileSystem.Parameters)
FileSystemS3.update_forward_refs(Parameters=FileSystemS3.Parameters)
ZarrLocalStorage.update_forward_refs(Parameters=ZarrLocalStorage.Parameters)
