import logging
import shutil
import tempfile
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from time import time
from typing import Any, Dict, List, Optional, Tuple, Union

import boto3
import botocore.exceptions
import xarray as xr
from pydantic import BaseSettings, Field, root_validator, validator
from tstring import Template

from ..utils import get_fields_from_datastream, get_filename, get_fields_from_dataset
from .base import Storage
from .handlers import FileHandler, NetCDFHandler, ZarrHandler

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
    # NOTE: StorageFile or similar work will likely require the dataset object or dataset
    # config object, which means fetch data and other methods that need to search for data
    # will also need to be provided with this info.

    class Parameters(BaseSettings):
        storage_root: Path = Path.cwd() / "storage" / "root"
        """The path on disk where data and ancillary files will be saved to. Defaults to
        the `storage/root` folder in the active working directory. The directory is
        created as this parameter is set, if the directory does not already exist."""

        data_folder: Path = Field(Path("data"))
        """The directory under storage_root/ where datastream data folders and files
        should be saved to. Defaults to `data/`."""

        ancillary_folder: Path = Path("ancillary")
        """The directory under storage_root/ where datastream ancillary folders and
        files should be saved to. This is primarily used for plots. Defaults to
        `ancillary/`."""

        data_storage_path: Path = Path("{storage_root}/{data_folder}/{datastream}")
        """The directory structure that should be used when data files are saved. Allows
        substitution of the following parameters using curly braces '{}':
        
        * ``storage_root``: the value from the ``storage_root`` parameter.
        * ``data_folder``:  the value from the ``data_folder`` parameter.
        * ``ancillary_folder``:  the value from the ``ancillary_folder`` parameter.
        * ``datastream``: the ``datastream`` as defined in the dataset configuration file.
        * ``location_id``: the ``location_id`` as defined in the dataset configuration file.
        * ``data_level``: the ``data_level`` as defined in the dataset configuration file.
        * ``year``: the year of the first timestamp in the file.
        * ``month``: the month of the first timestamp in the file.
        * ``day``: the day of the first timestamp in the file.
        * ``extension``: the file extension used by the output file writer.

        Defaults to ``{storage_root}/{data_folder}/{datastream}``.
        """

        ancillary_storage_path: Path = Path(
            "{storage_root}/{ancillary_folder}/{datastream}"
        )
        """The directory structure that should be used when ancillary files are saved.
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``storage_root``: the value from the ``storage_root`` parameter.
        * ``data_folder``:  the value from the ``data_folder`` parameter.
        * ``ancillary_folder``:  the value from the ``ancillary_folder`` parameter.
        * ``datastream``: the ``datastream`` as defined in the dataset configuration file.
        * ``location_id``: the ``location_id`` as defined in the dataset configuration file.
        * ``data_level``: the ``data_level`` as defined in the dataset configuration file.
        * ``ext``: the file extension (e.g., 'png', 'gif').
        * ``year``: the year of the first timestamp in the file.
        * ``month``: the month of the first timestamp in the file.
        * ``day``: the day of the first timestamp in the file.

        Defaults to ``{storage_root}/{ancillary_folder}/{datastream}``.
        """

        file_timespan: Optional[str] = None  # Unused

        merge_fetched_data_kwargs: Dict[str, Any] = dict()
        """Keyword arguments passed to xr.merge.
        
        Note that this will only be called if the DataReader returns a dictionary of
        xr.Datasets for a single input key."""

        @validator("storage_root")
        def _ensure_storage_root_exists(cls, storage_root: Path) -> Path:
            if not storage_root.is_dir():
                logger.info("Creating storage root at: %s", storage_root.as_posix())
                storage_root.mkdir(parents=True)
            return storage_root

        @root_validator()
        def _resolve_static_substitutions(
            cls, values: Dict[str, Any]
        ) -> Dict[str, Any]:
            substitutions = {
                "storage_root": str(values["storage_root"]),
                "data_folder": str(values["data_folder"]),
                "ancillary_folder": str(values["ancillary_folder"]),
            }

            values["data_storage_path"] = Path(
                Template(str(values["data_storage_path"])).substitute(
                    substitutions, allow_missing=True
                )
            )
            values["ancillary_storage_path"] = Path(
                Template(str(values["ancillary_storage_path"])).substitute(
                    substitutions, allow_missing=True
                )
            )

            return values

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
        filepath = self._get_dataset_filepath(dataset)
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
        unresolved = self.parameters.data_storage_path.as_posix()
        extension = self.handler.writer.file_extension
        extension = extension if not extension.startswith(".") else extension[1:]
        semi_resolved = Template(unresolved).substitute(
            get_fields_from_datastream(datastream),
            allow_missing=True,
            extension=extension,
        )
        root_path, pattern = self._extract_time_substitutions(semi_resolved, start, end)
        filepaths = list(root_path.glob(pattern))  # FIXME
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

    def _substitute_ancillary_filepath_parts(
        self, filepath: Path, datastream: Optional[str]
    ) -> Path:
        ancillary_stub_path = Template(
            self.parameters.ancillary_storage_path.as_posix()
        )

        start: Optional[datetime] = None

        try:
            # TODO: Extract logic for splitting filepath/name into component parts
            # Filepath should be like loc.name[-qual][temp].lvl.date.time[.title].ext
            #                         ^^^^^^ datastream ^^^^^^^
            filename_parts = filepath.name.split(".")
            assert len(filename_parts) >= 5

            loc_id = filename_parts[0]
            # dataset_name = filename_parts[1].split("-")[0]
            data_level = filename_parts[2]
            datastream = datastream or ".".join(filename_parts[:3])
            start_date_time = ".".join(filename_parts[3:5])
            start = datetime.strptime(start_date_time, "%Y%m%d.%H%M%S")
        except AssertionError:  # filename not standardized; require datastream
            logger.warning(
                "Attempting to store file with unstandardized filename. This will be"
                " deprecated in a future release of tsdat. Please use"
                " `tsdat.utils.get_filename()` to get a standardized filename for your"
                " ancillary file."
            )
            if datastream is None:
                raise ValueError(
                    "Argument 'datastream' must be provided to the"
                    " 'save_ancillary_file()' method if not saving an ancillary file"
                    " with a tsdat-standardized name."
                )
            loc_id, _, data_level = datastream.split(".")

        def get_time_fmt(fmt: str) -> str:
            if start is None:
                raise ValueError(
                    f"Substitution '{fmt}' cannot be used with an unstandardized"
                    " filename. Please modify the `ancillary_storage_path` config"
                    " parameter or use `tsdat.utils.get_filename()` to get a"
                    " standardized filename for your ancillary file."
                )
            return start.strftime(fmt)

        return Path(
            ancillary_stub_path.substitute(
                datastream=datastream,
                location_id=loc_id,
                data_level=data_level,
                ext=filepath.suffix,
                year=lambda: get_time_fmt("%Y"),
                month=lambda: get_time_fmt("%m"),
                day=lambda: get_time_fmt("%d"),
            )
        )

    def _get_dataset_filepath(self, dataset: xr.Dataset) -> Path:
        data_stub_path = Template(self.parameters.data_storage_path.as_posix())
        extension = self.handler.writer.file_extension
        extension = extension if not extension.startswith(".") else extension[1:]
        datastream_dir = Path(
            data_stub_path.substitute(
                get_fields_from_dataset(dataset),
                extension=extension,
            )
        )
        return datastream_dir / get_filename(dataset, extension)

    def _get_ancillary_filepath(
        self, filepath: Path, datastream: Optional[str] = None
    ) -> Path:
        return (
            self._substitute_ancillary_filepath_parts(filepath, datastream=datastream)
            / filepath.name
        )

    def _extract_time_substitutions(
        self, template_str: str, start: datetime, end: datetime
    ) -> Tuple[Path, str]:
        """Extracts the root path above unresolved time substitutions and provides a pattern to search below that."""
        year = start.strftime("%Y") if start.year == end.year else "*"
        month = (
            start.strftime("%m") if year != "*" and start.month == end.month else "*"
        )
        resolved = Template(template_str).substitute(year=year, month=month, day="*")
        if (split := resolved.find("*")) != -1:
            return Path(resolved[:split]), resolved[split:] + "/*"
        return Path(resolved), "*"


class FileSystemS3(FileSystem):
    """------------------------------------------------------------------------------------
    Handles data storage and retrieval for file-based data formats in an AWS S3 bucket.

    Args:
        parameters (Parameters): File-system and AWS-specific parameters, such as the root
            path to where files should be saved, or additional keyword arguments to
            specific functions used by the storage API. See the FileSystemS3.Parameters
            class for more details.
        handler (FileHandler): The FileHandler class that should be used to handle data
            I/O within the storage API.

    ------------------------------------------------------------------------------------"""

    class Parameters(FileSystem.Parameters):  # type: ignore
        """Additional parameters for S3 storage.

        Note that all settings and parameters from ``Filesystem.Parameters`` are also
        supported by ``FileSystemS3.Parameters``."""

        storage_root: Path = Field(Path("storage/root"), env="TSDAT_STORAGE_ROOT")
        """The path on disk where data and ancillary files will be saved to.
        
        Note:
            This parameter can also be set via the ``TSDAT_STORAGE_ROOT`` environment
            variable.

        Defaults to the ``storage/root`` folder in the top level of the storage bucket.
        """

        bucket: str = Field("tsdat-storage", env="TSDAT_S3_BUCKET_NAME")
        """The name of the S3 bucket that the storage class should use.
        
        Note:
            This parameter can also be set via the ``TSDAT_S3_BUCKET_NAME`` environment
            variable.
        """

        region: str = Field("us-west-2", env="AWS_DEFAULT_REGION")
        """The AWS region of the storage bucket.
        
        Note:
            This parameter can also be set via the ``AWS_DEFAULT_REGION`` environment
            variable.
        
        Defaults to ``us-west-2``."""

        merge_fetched_data_kwargs: Dict[str, Any] = dict()
        """Keyword arguments to xr.merge. This will only be called if the
        DataReader returns a dictionary of xr.Datasets for a single saved file."""

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore

    @validator("parameters")
    def _check_authentication(cls, parameters: Parameters):
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
    def _ensure_bucket_exists(cls, parameters: Parameters):
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
    def _session(self):
        return FileSystemS3._get_session(
            region=self.parameters.region, timehash=FileSystemS3._get_timehash()
        )

    @property
    def _bucket(self):
        s3 = self._session.resource("s3", region_name=self.parameters.region)  # type: ignore
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
        standard_fpath = self._get_dataset_filepath(dataset)

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_filepath = Path(tmp_dir) / standard_fpath
            tmp_filepath.parent.mkdir(parents=True, exist_ok=True)

            self.handler.writer.write(dataset, tmp_filepath)

            for filepath in Path(tmp_dir).glob("**/*"):
                if filepath.is_dir():
                    continue
                s3_filepath = filepath.relative_to(tmp_dir).as_posix()
                self._bucket.upload_file(Filename=filepath.as_posix(), Key=s3_filepath)
                logger.info(
                    "Saved %s data file to s3://%s/%s",
                    datastream,
                    self.parameters.bucket,
                    s3_filepath,
                )

    def save_ancillary_file(self, filepath: Path, datastream: str):
        s3_filepath = self._get_ancillary_filepath(filepath, datastream)
        self._bucket.upload_file(Filename=str(filepath), Key=str(s3_filepath))
        logger.info("Saved %s ancillary file to: %s", filepath.name, str(s3_filepath))

    def _find_data(self, start: datetime, end: datetime, datastream: str) -> List[Path]:
        prefix = str(self.parameters.storage_root / "data" / datastream) + "/"
        objects = self._bucket.objects.filter(Prefix=prefix)
        filepaths = [
            Path(obj.key) for obj in objects if obj.key.endswith(self.handler.extension)
        ]
        return self._filter_between_dates(filepaths, start, end)

    def _open_data_files(self, *filepaths: Path) -> List[xr.Dataset]:
        dataset_list: List[xr.Dataset] = []
        with tempfile.TemporaryDirectory() as tmp_dir:
            for s3_filepath in filepaths:
                tmp_filepath = str(Path(tmp_dir) / s3_filepath.name)
                self._bucket.download_file(
                    Key=str(s3_filepath),
                    Filename=tmp_filepath,
                )
                data = self.handler.reader.read(tmp_filepath)
                if isinstance(data, dict):
                    data = xr.merge(data.values())  # type: ignore
                data = data.compute()  # type: ignore
                dataset_list.append(data)
        return dataset_list

    def _exists(self, key: Union[Path, str]) -> bool:
        return self._get_obj(str(key)) is not None

    def _get_obj(self, key: Union[Path, str]):
        objects = self._bucket.objects.filter(Prefix=str(key))
        try:
            return next(obj for obj in objects if obj.key == str(key))
        except StopIteration:
            return None


class ZarrLocalStorage(FileSystem):
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

    class Parameters(FileSystem.Parameters):
        data_storage_path: Path = Path(
            "{storage_root}/{data_folder}"  # /{datastream}.{extension}
        )
        """The directory structure that should be used when data files are saved. Allows
        substitution of the following parameters using curly braces '{}':
        
        * ``storage_root``: the value from the ``storage_root`` parameter.
        * ``data_folder``:  the value from the ``data_folder`` parameter.
        * ``ancillary_folder``:  the value from the ``ancillary_folder`` parameter.
        * ``datastream``: the ``datastream`` as defined in the dataset configuration file.
        * ``location_id``: the ``location_id`` as defined in the dataset configuration file.
        * ``data_level``: the ``data_level`` as defined in the dataset configuration file.
        * ``year``: the year of the first timestamp in the file.
        * ``month``: the month of the first timestamp in the file.
        * ``day``: the day of the first timestamp in the file.
        * ``extension``: the file extension used by the output file writer.

        Defaults to ``{storage_root}/{data_folder}/{datastream}.{extension}``.
        """

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore
    handler: ZarrHandler = Field(default_factory=ZarrHandler)

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
        data_files = self._find_data(start=start, end=end, datastream=datastream)
        full_dataset = self.handler.reader.read(data_files[0].as_posix())
        dataset_in_range = full_dataset.sel(time=slice(start, end))
        return dataset_in_range.compute()  # type: ignore

    def _get_dataset_filepath(self, dataset: xr.Dataset) -> Path:
        data_stub_path = Template(self.parameters.data_storage_path.as_posix())
        extension = self.handler.writer.file_extension
        extension = extension if not extension.startswith(".") else extension[1:]
        datastream_dir = Path(
            data_stub_path.substitute(
                get_fields_from_dataset(dataset),
                extension=extension,
            )
        )
        return datastream_dir / f"{dataset.attrs['datastream']}.{extension}"

    def _find_data(self, start: datetime, end: datetime, datastream: str) -> List[Path]:
        unresolved = self.parameters.data_storage_path.as_posix()
        extension = self.handler.writer.file_extension
        extension = extension if not extension.startswith(".") else extension[1:]
        semi_resolved = Template(unresolved).substitute(
            get_fields_from_datastream(datastream),
            allow_missing=True,
            extension=extension,
        )
        root_path, pattern = self._extract_time_substitutions(semi_resolved, start, end)
        pattern += f"{datastream}.{extension}"  # zarr folder for the datastream, not included by pattern
        return list(root_path.glob(pattern))

    def _extract_time_substitutions(
        self, template_str: str, start: datetime, end: datetime
    ) -> Tuple[Path, str]:
        """Extracts the root path above unresolved time substitutions and provides a pattern to search below that."""
        year = start.strftime("%Y") if start.year == end.year else "*"
        month = (
            start.strftime("%m") if year != "*" and start.month == end.month else "*"
        )
        resolved = Template(template_str).substitute(year=year, month=month, day="*")
        if (split := resolved.find("*")) != -1:
            return Path(resolved[:split]), resolved[split:]
        return Path(resolved), ""


# HACK: Update forward refs to get around error I couldn't replicate with simpler code
# "pydantic.errors.ConfigError: field "parameters" not yet prepared so type is still a ForwardRef..."
FileSystem.update_forward_refs(Parameters=FileSystem.Parameters)
FileSystemS3.update_forward_refs(Parameters=FileSystemS3.Parameters)
ZarrLocalStorage.update_forward_refs(Parameters=ZarrLocalStorage.Parameters)
