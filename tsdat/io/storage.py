# TODO: Implement FileSystemS3
import logging
import os
import shutil
import xarray as xr
from datetime import datetime
from pydantic import BaseSettings, validator
from pathlib import Path
from typing import Any, Dict, List, Optional
from .base import Storage
from .handlers import FileHandler, ZarrHandler
from ..utils import get_filename
import io
import boto3


__all__ = ["FileSystem", "S3Storage"]

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
        @classmethod
        def _ensure_storage_root_exists(cls, storage_root: Path) -> Path:
            if not storage_root.is_dir():
                logger.info("Creating storage root at: %s", storage_root.as_posix())
                storage_root.mkdir(parents=True)
            return storage_root

    parameters: Parameters = Parameters()
    handler: FileHandler

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


s3_Path = str  # alias


class S3Storage(FileSystem):

    class Parameters(FileSystem.Parameters):
        bucket: str
        region: str = "us-west-2"

    @staticmethod
    def _check_aws_credentials():
        try:
            aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
            aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
            aws_session_token = os.environ["AWS_SESSION_TOKEN"]
        except Exception as e:
            logger.warning("Environment variable for AWS credentials is not configured")
            logger.warning(e)

    def _delete_all_objects_under_prefix(self, prefix):
        s3 = self._get_s3_resource()
        test_bucket = s3.Bucket(self.parameters.bucket)
        for obj in test_bucket.objects.filter(Prefix=prefix):
            obj.delete()

    def _get_s3_client(self):  # TODO: use singleton pattern
        self._check_aws_credentials()
        client = boto3.client('s3')
        return client

    def _get_s3_resource(self):  # TODO: use singleton pattern
        self._check_aws_credentials()
        resource = boto3.resource('s3')
        return resource

    parameters: Parameters

    def save_data(self, dataset: xr.Dataset):
        """-----------------------------------------------------------------------------
                Saves a dataset to the s3 bucket. (save_data counterpart)

                At a minimum, the dataset must have a 'datastream' global attribute and must
                have a 'time' variable with a np.datetime64-like data type.

                Args:
                    dataset (xr.Dataset): The dataset to save.

                -----------------------------------------------------------------------------"""
        return self.save_data_s3(dataset)

    def fetch_data(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        """-----------------------------------------------------------------------------
                Gets data from AWS S3 for a given datastream between a specified time range. (fetch_data counterpart)

                Note: this method is not smart; it searches for the appropriate data files using
                their filenames and does not filter within each data file.

                Args:
                    start (datetime): The minimum datetime to fetch.
                    end (datetime): The maximum datetime to fetch.
                    datastream (str): The datastream id to search for.

                Returns:
                    xr.Dataset: A dataset containing (after searching and merging) all the data
                    in the storage area that spans the specified datetimes.

                -----------------------------------------------------------------------------"""
        return self.fetch_data_s3(start, end, datastream)

    def save_ancillary_file(self, filepath: Path, datastream: str):
        """-----------------------------------------------------------------------------
                Saves an ancillary filepath to the datastream's ancillary storage area.

                Args:
                    filepath (Path): The path to the ancillary file. (Note: assuming filepath is at local)
                    datastream (str): The datastream that the file is related to.

                -----------------------------------------------------------------------------"""
        path_src: str = str(filepath)
        with open(filepath, 'r') as f:
            output = f.read()
        self._put_object_s3(object_bytes=output, file_name_on_s3=path_src)
        return self.save_ancillary_file_s3(path_src, datastream, True)

    def _put_object_s3(self, object_bytes: bytes, file_name_on_s3: s3_Path):

        client = self._get_s3_client()
        bucket = self.parameters.bucket
        response = client.put_object(
            Body=object_bytes,
            Bucket=bucket,
            Key=file_name_on_s3,
        )
        # print(response)

    def save_data_s3(self, dataset: xr.Dataset):
        """-----------------------------------------------------------------------------
                Saves a dataset to the s3 bucket. (save_data counterpart)

                At a minimum, the dataset must have a 'datastream' global attribute and must
                have a 'time' variable with a np.datetime64-like data type.

                Args:
                    dataset (xr.Dataset): The dataset to save.

                -----------------------------------------------------------------------------"""

        datastream = dataset.attrs["datastream"]
        filepath = self._get_dataset_filepath(dataset, datastream)

        file_body_to_upload: bytes = dataset.to_netcdf(path=None)  # return ``bytes` if path is None`
        file_name_on_s3: s3_Path = str(filepath)

        # put_object to s3 directly from memory
        self._put_object_s3(object_bytes=file_body_to_upload, file_name_on_s3=file_name_on_s3)
        logger.info("Saved %s dataset to AWS S3 in bucket %s at %s", datastream, self.parameters.bucket, file_name_on_s3)

    def fetch_data_s3(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        """-----------------------------------------------------------------------------
                Gets data from AWS S3 for a given datastream between a specified time range. (fetch_data counterpart)

                Note: this method is not smart; it searches for the appropriate data files using
                their filenames and does not filter within each data file.

                Args:
                    start (datetime): The minimum datetime to fetch.
                    end (datetime): The maximum datetime to fetch.
                    datastream (str): The datastream id to search for.

                Returns:
                    xr.Dataset: A dataset containing (after searching and merging) all the data 
                    in the storage area that spans the specified datetimes.

                -----------------------------------------------------------------------------"""
        data_files_s3 = self._find_data_at_s3(start, end, datastream)
        datasets = self._open_data_files_s3(*data_files_s3)

        return xr.merge(datasets, **self.parameters.merge_fetched_data_kwargs)  # type: ignore

    def _create_bucket(self):
        # TODO: create a bucket if not exists
        pass

    def _is_file_exist_s3(self, key_name: s3_Path) -> bool:
        s3 = self._get_s3_resource()
        bucket_name = self.parameters.bucket
        s3_object_info = s3.Object(bucket_name, key_name)
        try:
            result = s3_object_info.get()
            # print(result)
            return True
        except Exception as e:  # catch NoSuchKey
            return False

    def _open_data_files_s3(self, *filepaths_s3: str) -> List[xr.Dataset]:
        dataset_list: List[xr.Dataset] = []
        # download object to memory
        s3 = self._get_s3_resource()
        my_bucket = s3.Bucket(self.parameters.bucket)

        for filepath in filepaths_s3:
            buf = io.BytesIO()
            my_bucket.download_fileobj(filepath, buf)
            file_content_bytes = buf.getvalue()
            data = xr.load_dataset(file_content_bytes)
            # data = self.handler.reader.read(filepath.as_posix())
            if isinstance(data, dict):
                data = xr.merge(data.values())  # type: ignore
            dataset_list.append(data)
        return dataset_list

    def list_files_s3(self, prefix: s3_Path) -> List[s3_Path]:
        """-----------------------------------------------------------------------------
                List objects/files at s3 at certain prefix/dir

                Args:
                    prefix (s3_Path): The prefix/directory to query.

                Returns:
                    List[s3_Path]: A list of available files under certain directory

                -----------------------------------------------------------------------------"""
        s3 = self._get_s3_resource()
        my_bucket = s3.Bucket(self.parameters.bucket)
        response = my_bucket.objects.filter(Prefix=prefix)  # query object info at s3 bucket
        filepaths_at_s3: List[str] = [object_summary.key for object_summary in response]
        return filepaths_at_s3

    def _find_data_at_s3(self, start: datetime, end: datetime, datastream: str) -> List[s3_Path]:
        data_dirpath = self.parameters.storage_root / "data" / datastream

        prefix = str(data_dirpath)
        filepaths_at_s3 = self.list_files_s3(prefix)

        valid_filepaths_at_s3 = self._filter_between_dates(list(map(Path, filepaths_at_s3)), start, end)
        return list(map(str, valid_filepaths_at_s3))

    def save_ancillary_file_s3(self, path_src: s3_Path, datastream: str, is_src_temp: bool=False):
        """-----------------------------------------------------------------------------
        Saves an ancillary filepath to the datastream's ancillary storage area.

        Args:
            path_src (s3_Path): The path to the ancillary file.
            datastream (str): The datastream that the file is related to.
            is_src_temp (bool), False: Flag to indicate if the file at path_src is temporary. If so then delete it.

        -----------------------------------------------------------------------------"""
        path_dst: s3_Path = str(self._get_ancillary_filepath(Path(path_src), datastream))
        s3 = self._get_s3_resource()
        bucket_name = self.parameters.bucket
        copy_source = {
            'Bucket': bucket_name,
            'Key': path_src
        }
        my_bucket = s3.Bucket(bucket_name)
        my_bucket.copy(copy_source, path_dst)
        logger.info("Saved ancillary to AWS S3 to %s, in bucket %s", path_dst, bucket_name)

        # if the file at path_src is temporary, clean up tmp file
        if is_src_temp:
            self._delete_all_objects_under_prefix(prefix=path_src)


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

    handler: ZarrHandler = ZarrHandler()
    parameters: Parameters = Parameters()

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

