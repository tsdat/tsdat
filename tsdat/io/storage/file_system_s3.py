import logging
import re
import tempfile
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from time import time
from typing import Any, Dict, List, Protocol, Union

import xarray as xr
from pydantic import Field, validator

from ...utils import (
    get_file_datetime_str,
)
from .file_system import FileSystem

logger = logging.getLogger(__name__)


class S3Object(Protocol):
    key: str
    last_modified: datetime


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
        """Returns the datetime of the last modification to the datastream's storage
        area."""
        filepath_glob = self.data_filepath_template.substitute(
            self._get_substitutions(datastream=datastream),
            allow_missing=True,
            fill=".*",
        )
        s3_objects = self._get_matching_s3_objects(filepath_glob)

        last_modified = None
        for obj in s3_objects:
            if obj.last_modified is not None:
                mod_time = obj.last_modified.astimezone(timezone.utc)
                last_modified = (
                    mod_time if last_modified is None else max(last_modified, mod_time)
                )
        return last_modified

    def modified_since(
        self, datastream: str, last_modified: datetime
    ) -> List[datetime]:
        """Returns the data datetimes of all files modified after the specified time."""
        filepath_glob = self.data_filepath_template.substitute(
            self._get_substitutions(datastream=datastream),
            allow_missing=True,
            fill=".*",
        )
        s3_objects = self._get_matching_s3_objects(filepath_glob)
        return [
            datetime.strptime(get_file_datetime_str(obj.key), "%Y%m%d.%H%M%S")
            for obj in s3_objects
            if (
                obj.last_modified is not None
                and obj.last_modified.astimezone(timezone.utc) > last_modified
            )
        ]

    def _get_matching_s3_objects(self, filepath_glob: str) -> List[S3Object]:
        assert (
            ".*" in filepath_glob or "[0-9]{6}" in filepath_glob  # need some regex
        ), "Naming scheme must distinguish between files within the same datastream"

        split_idx = filepath_glob.rindex("/")  # default
        if ".*" in filepath_glob:
            split_idx = min(split_idx, filepath_glob.index(".*"))
        if "[0-9]{6}" in filepath_glob:
            split_idx = min(split_idx, filepath_glob.index("[0-9]"))

        prefix, glob = filepath_glob[:split_idx], re.compile(filepath_glob[split_idx:])

        matches: list[S3Object] = []
        for obj in self._bucket.objects.filter(Prefix=prefix):
            suffix = obj.key[len(prefix) :]
            if glob.fullmatch(suffix):
                matches.append(obj)

        return matches

    def save_ancillary_file(self, filepath: Path, target_path: Path):  # type: ignore
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
        filepath = Path(
            self.data_filepath_template.substitute(
                self._get_substitutions(dataset=dataset),
                allow_missing=False,
            )
        )
        with tempfile.TemporaryDirectory() as tmp_dir:
            self.handler.writer.write(dataset, Path(tmp_dir) / filepath.name)
            for file in Path(tmp_dir).glob("**/*"):
                if file.is_file():
                    key = (filepath.parent / file.relative_to(tmp_dir)).as_posix()
                    self._bucket.upload_file(Filename=file.as_posix(), Key=key)
                    logger.info(
                        "Saved %s data file to s3://%s/%s",
                        dataset.attrs["datastream"],
                        self.parameters.bucket,
                        key,
                    )
        return None

    def _find_data(
        self,
        start: datetime,
        end: datetime,
        datastream: str,
        metadata_kwargs: Dict[str, str] | None = None,
        **kwargs: Any,
    ) -> List[Path]:
        substitutions = self._get_substitutions(
            datastream=datastream,
            time_range=(start, end),
            extra=metadata_kwargs,
        )
        filepath_glob = self.data_filepath_template.substitute(
            substitutions,
            allow_missing=True,
            fill=".*",
        )
        matches = self._get_matching_s3_objects(filepath_glob)
        paths = [Path(obj.key) for obj in matches]
        return self._filter_between_dates(paths, start, end)

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


# TODO:
#  HACK: Update forward refs to get around error I couldn't replicate with simpler code
#  "pydantic.errors.ConfigError: field "parameters" not yet prepared
#  so type is still a ForwardRef..."
FileSystemS3.update_forward_refs(Parameters=FileSystemS3.Parameters)
