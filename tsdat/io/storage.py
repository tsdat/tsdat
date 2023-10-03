import logging
import re
import shutil
import tempfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from time import time
from typing import Any, Dict, Iterable, List, Tuple, Union

import xarray as xr
from pydantic import Field, validator
from tsdat.tstring import Template

from ..utils import (
    get_fields_from_dataset,
    get_fields_from_datastream,
    get_file_datetime_str,
)
from .base import Storage
from .handlers import FileHandler, NetCDFHandler, ZarrHandler

__all__ = ["FileSystem", "FileSystemS3", "ZarrLocalStorage"]


logger = logging.getLogger(__name__)


class FileSystem(Storage):
    """Handles data storage and retrieval for file-based data formats.

    Formats that write to directories (such as zarr) are not supported by the FileSystem
    storage class.

    Args:
        parameters (Parameters): File-system specific parameters, such as the root path
            to where files should be saved, or additional keyword arguments to specific
            functions used by the storage API. See the FileSystemStorage.Parameters
            class for more details.
        handler (FileHandler): The FileHandler class that should be used to handle data
            I/O within the storage API.
    """

    class Parameters(Storage.Parameters):
        data_storage_path: Path = Path("data/{location_id}/{datastream}")
        """The directory structure under storage_root where ancillary files are saved.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``storage_root``: the value from the ``storage_root`` parameter.
        * ``datastream``: the ``datastream`` as defined in the dataset config file.
        * ``location_id``: the ``location_id`` as defined in the dataset config file.
        * ``data_level``: the ``data_level`` as defined in the dataset config file.
        * ``year``: the year of the first timestamp in the file.
        * ``month``: the month of the first timestamp in the file.
        * ``day``: the day of the first timestamp in the file.
        * ``extension``: the file extension used by the output file writer.

        Defaults to ``data/{location_id}/{datastream}``.
        """

        data_filename_template: str = "{datastream}.{date_time}.{extension}"
        """Template string to use for data filenames.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``ext``: the file extension from the storage data handler
        * ``datastream`` from the dataset's global attributes
        * ``location_id`` from the dataset's global attributes
        * ``data_level`` from the dataset's global attributes
        * ``date_time``: the first timestamp in the file formatted as "YYYYMMDD.hhmmss"
        * Any other global attribute that has a string or integer data type.
        
        At a minimum the template must include ``{date_time}``.
        """

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

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore
    handler: FileHandler = Field(default_factory=NetCDFHandler)

    def save_ancillary_file(
        self, filepath: Path, target_path: Union[Path, None] = None
    ):
        """Saves an ancillary filepath to the datastream's ancillary storage area.

        NOTE: In most cases this function should not be used directly. Instead, prefer
        using the ``self.uploadable_dir(*args, **kwargs)`` method.

        Args:
            filepath (Path): The path to the ancillary file. This is expected to have
                a standardized filename and should be saved under the ancillary storage
                path.
            target_path (str): The path to where the data should be saved.
        """
        target_path.parent.mkdir(exist_ok=True, parents=True)
        saved_filepath = shutil.copy2(filepath, target_path)
        logger.info("Saved ancillary file to: %s", saved_filepath)

    def save_data(self, dataset: xr.Dataset, **kwargs: Any):
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

    def fetch_data(
        self,
        start: datetime,
        end: datetime,
        datastream: str,
        metadata_kwargs: Union[Dict[str, str], None] = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Fetches data for a given datastream between a specified time range.

        Args:
            start (datetime): The minimum datetime to fetch.
            end (datetime): The maximum datetime to fetch.
            datastream (str): The datastream id to search for.
            metadata_kwargs (dict[str, str], optional): Metadata substitutions to help
                resolve the data storage path. This is only required if the template
                data storage path includes any properties other than datastream or
                fields contained in the datastream. Defaults to None.

        Returns:
            xr.Dataset: A dataset containing all the data in the storage area that spans
            the specified datetimes.

        -----------------------------------------------------------------------------"""
        if metadata_kwargs is None:
            metadata_kwargs = {}
        metadata_kwargs = {
            "datastream": datastream,
            **get_fields_from_datastream(datastream),
            **metadata_kwargs,
        }
        data_files = self._find_data(
            start,
            end,
            datastream,
            metadata_kwargs=metadata_kwargs,
        )
        datasets = self._open_data_files(*data_files)
        dataset = xr.merge(datasets, **self.parameters.merge_fetched_data_kwargs)  # type: ignore
        if not dataset:
            logger.warning(
                "No data found for %s in range %s - %s", datastream, start, end
            )
            return dataset  # empty
        return dataset.sel(time=slice(start, end))

    def _find_data(
        self,
        start: datetime,
        end: datetime,
        datastream: str,
        metadata_kwargs: Dict[str, str],
        **kwargs: Any,
    ) -> List[Path]:
        dir_template = Template(self.parameters.data_storage_path.as_posix())
        extension = self.handler.writer.file_extension
        semi_resolved = dir_template.substitute(
            {
                **dict(
                    datastream=datastream,
                    extension=extension,
                    ext=extension,
                ),
                **metadata_kwargs,
            },
            allow_missing=True,
        )
        dirpath, pattern = self._extract_time_substitutions(semi_resolved, start, end)
        dirpath = self.parameters.storage_root / dirpath
        filepaths = (p for p in dirpath.glob(pattern))
        return self._filter_between_dates(filepaths, start, end)

    def _filter_between_dates(
        self, filepaths: Iterable[Path], start: datetime, end: datetime
    ) -> List[Path]:
        start_date_str = start.strftime("%Y%m%d.%H%M%S")
        end_date_str = end.strftime("%Y%m%d.%H%M%S")

        valid_filepaths: List[Path] = []
        for filepath in filepaths:
            file_date_str = get_file_datetime_str(filepath)
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

    def _get_dataset_filepath(self, dataset: xr.Dataset) -> Path:
        extension = self.handler.writer.file_extension
        substitutions = get_fields_from_dataset(dataset)
        substitutions.update(extension=extension, ext=extension)
        data_dir = self._get_data_directory(substitutions)
        filename_template = Template(self.parameters.data_filename_template)
        filename = filename_template.substitute(substitutions)
        return data_dir / filename

    def _get_data_directory(self, substitutions: Dict[str, str]) -> Path:
        dir_template = Template(self.parameters.data_storage_path.as_posix())
        dirpath = dir_template.substitute(substitutions)
        return self.parameters.storage_root / dirpath

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
    """Handles data storage and retrieval for file-based data in an AWS S3 bucket.

    Args:
        parameters (Parameters): File-system and AWS-specific parameters, such as the
            path to where files should be saved or additional keyword arguments to
            specific functions used by the storage API. See the FileSystemS3.Parameters
            class for more details.
        handler (FileHandler): The FileHandler class that should be used to handle data
            I/O within the storage API.
    """

    class Parameters(FileSystem.Parameters):  # type: ignore
        """Additional parameters for S3 storage.

        Note that all settings and parameters from ``Filesystem.Parameters`` are also
        supported by ``FileSystemS3.Parameters``."""

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

        @validator("storage_root")
        def _ensure_storage_root_exists(cls, storage_root: Path) -> Path:
            return storage_root  # HACK: Don't run parent validator to create storage root file

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore

    @validator("parameters")
    def _check_authentication(cls, parameters: Parameters):
        import botocore.exceptions

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
        import botocore.exceptions

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

    # TODO: use cachetools.func.ttl_cache() so we don't create lots of bucket resources
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

        ------------------------------------------------------------------------------------
        """
        import boto3

        del timehash
        return boto3.session.Session(region_name=region)

    @staticmethod
    def _get_timehash(seconds: int = 3600) -> int:
        return round(time() / seconds)

    def last_modified(self, datastream: str) -> Union[datetime, None]:
        """Returns the datetime of the last modification to the datastream's storage area."""
        substitutions = get_fields_from_datastream(datastream)
        substitutions["datastream"] = datastream
        prefix = self._get_data_directory(substitutions).as_posix()

        last_modified = None
        for obj in self._bucket.objects.filter(Prefix=prefix):
            if obj.last_modified is not None:
                last_modified = (
                    obj.last_modified.astimezone(timezone.utc)
                    if last_modified is None
                    else max(last_modified, obj.last_modified)
                )
        return last_modified

    def modified_since(
        self, datastream: str, last_modified: datetime
    ) -> List[datetime]:
        """Returns the data times of all files modified after the specified datetime."""
        substitutions = get_fields_from_datastream(datastream)
        substitutions["datastream"] = datastream
        prefix = self._get_data_directory(substitutions).as_posix()
        return [
            datetime.strptime(get_file_datetime_str(obj.key), "%Y%m%d.%H%M%S")
            for obj in self._bucket.objects.filter(Prefix=prefix)
            if obj.last_modified is not None
            and obj.last_modified.astimezone(timezone.utc) > last_modified
        ]

    def save_ancillary_file(
        self, filepath: Path, target_path: Union[Path, None] = None
    ):
        """Saves an ancillary filepath to the datastream's ancillary storage area.

        NOTE: In most cases this function should not be used directly. Instead, prefer
        using the ``self.uploadable_dir(*args, **kwargs)`` method.

        Args:
            filepath (Path): The path to the ancillary file. This is expected to have
                a standardized filename and should be saved under the ancillary storage
                path.
            target_path (str): The path to where the data should be saved.
        """
        self._bucket.upload_file(Filename=str(filepath), Key=target_path.as_posix())
        logger.info("Saved ancillary file to: %s", target_path.as_posix())

    def save_data(self, dataset: xr.Dataset, **kwargs: Any):
        datastream: str = dataset.attrs["datastream"]
        standard_fpath = self._get_dataset_filepath(dataset)
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_filepath = Path(tmp_dir) / standard_fpath.name
            self.handler.writer.write(dataset, tmp_filepath)
            for filepath in Path(tmp_dir).glob("**/*"):
                if filepath.is_dir():
                    continue
                s3_key = (
                    standard_fpath.parent / filepath.relative_to(tmp_dir)
                ).as_posix()
                self._bucket.upload_file(Filename=filepath.as_posix(), Key=s3_key)
                logger.info(
                    "Saved %s data file to s3://%s/%s",
                    datastream,
                    self.parameters.bucket,
                    s3_key,
                )

    def _find_data(
        self,
        start: datetime,
        end: datetime,
        datastream: str,
        metadata_kwargs: Dict[str, str],
        **kwargs: Any,
    ) -> List[Path]:
        dir_template = Template(self.parameters.data_storage_path.as_posix())
        extension = self.handler.writer.file_extension
        semi_resolved = dir_template.substitute(
            {
                **dict(
                    datastream=datastream,
                    extension=extension,
                    ext=extension,
                ),
                **metadata_kwargs,
            },
            allow_missing=True,
        )
        dirpath, pattern = self._extract_time_substitutions(semi_resolved, start, end)
        dirpath = self.parameters.storage_root / dirpath
        pattern = pattern.replace("*", ".*")

        objects = self._bucket.objects.filter(Prefix=dirpath.as_posix())
        filepaths = (Path(p.key) for p in objects if re.search(pattern, p.key))
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
                data = data.load()  # type: ignore
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
        data_storage_path: Path = Path("data/{location_id}")
        """The directory structure under storage_root where ancillary files are saved.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``storage_root``: the value from the ``storage_root`` parameter.
        * ``datastream``: the ``datastream`` as defined in the dataset config file.
        * ``location_id``: the ``location_id`` as defined in the dataset config file.
        * ``data_level``: the ``data_level`` as defined in the dataset config file.
        * ``year``: the year of the first timestamp in the file.
        * ``month``: the month of the first timestamp in the file.
        * ``day``: the day of the first timestamp in the file.
        * ``extension``: the file extension used by the output file writer.
        """

        data_filename_template: str = "{datastream}.{extension}"
        """Template string to use for data filenames.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``ext``: the file extension from the storage data handler
        * ``datastream`` from the dataset's global attributes
        * ``location_id`` from the dataset's global attributes
        * ``data_level`` from the dataset's global attributes
        * Any other global attribute that has a string or integer data type.
        """

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore
    handler: ZarrHandler = Field(default_factory=ZarrHandler)

    def _filter_between_dates(
        self, filepaths: Iterable[Path], start: datetime, end: datetime
    ) -> List[Path]:
        # Zarr filenames don't include dates. There should also only be one filepath
        # matching the data to fetch, so warn if otherwise
        zarr_files = sorted(filepaths)
        if len(zarr_files) > 1:
            logger.warning("More than zarr file found: %s", zarr_files)
        return zarr_files


# HACK: Update forward refs to get around error I couldn't replicate with simpler code
# "pydantic.errors.ConfigError: field "parameters" not yet prepared so type is still a ForwardRef..."
FileSystem.update_forward_refs(Parameters=FileSystem.Parameters)
FileSystemS3.update_forward_refs(Parameters=FileSystemS3.Parameters)
ZarrLocalStorage.update_forward_refs(Parameters=ZarrLocalStorage.Parameters)
