import bisect
import tarfile
import zipfile

import boto3
import datetime
import io
import os
from typing import List, Union, Dict

from tsdat.io import (
    DatastreamStorage,
    TemporaryStorage,
    DisposableLocalTempFile,
    DisposableStorageTempFileList,
    DisposableLocalTempFileList,
)
from tsdat.utils import DSUtil

SEPARATOR = "$$$"


class S3Path(str):
    """This class wraps a 'special' path string that lets us include the
    bucket name and region in the path, so that we can use it seamlessly
    in boto3 APIs.  We are creating our own string to hold the region,
    bucket & key (i.e., path), since boto3 needs all three in order to
    access a file.

    Example:
    .. code-block:: python

        s3_client = boto3.client('s3', region_name='eu-central-1')
        s3_client.download_file(bucket, key, download_path)

    :param bucket_name: The S3 bucket name where this file is located
    :type bucket_name: str
    :param bucket_path: The key to access this file in the bucket
    :type bucket_path: str, optional
    :param region_name: The AWS region where this file is located, defaults to None,
        which inherits the default configured region.
    :type region_name: str, optional
    """

    def __init__(
        self, bucket_name: str, bucket_path: str = "", region_name: str = None
    ):

        self._bucket_name = bucket_name
        self._region_name = region_name

        # Note that os.path.join returns Windows separators if
        # running on Windows, which causes problems in S3,
        # so we are making sure the path uses linux line separators
        bucket_path = bucket_path.replace("\\", "/")

        # Make sure that bucket path does not start with a slash!

        self._bucket_path = bucket_path

    def __str__(self):
        return self.bucket_path

    def __new__(cls, bucket_name: str, bucket_path: str, region_name: str = None):
        assert bucket_name
        assert bucket_path

        # Note that os.path.join returns Windows separators if
        # running on Windows, which causes problems in S3,
        # so we are making sure the path uses linux line separators
        bucket_path = bucket_path.replace("\\", "/")
        aws_path = ""

        if region_name:
            aws_path = f"{region_name}{SEPARATOR}{bucket_name}{SEPARATOR}{bucket_path}"
        else:
            aws_path = f"{bucket_name}{SEPARATOR}{bucket_path}"

        return str.__new__(cls, aws_path)

    @property
    def bucket_name(self):
        return self._bucket_name

    @property
    def bucket_path(self):
        return self._bucket_path

    @property
    def region_name(self):
        return self._region_name

    def join(self, *args):
        """Joins segments in an S3 path.  This method behaves
        exactly like os.path.join.

        :return: A New S3Path with the additional segments added.
        :rtype: S3Path
        """
        bucket_path = self.bucket_path

        for segment in args:
            bucket_path = os.path.join(bucket_path, segment)

        bucket_path = bucket_path.replace("\\", "/")
        return S3Path(self.bucket_name, bucket_path, self.region_name)


class AwsTemporaryStorage(TemporaryStorage):
    """Class used to store temporary files or perform
    fileystem actions on files other than datastream files
    that reside in the same AWS S3 bucket as the DatastreamStorage.
    This is a helper class intended to be used in the internals of
    pipeline implementations only.  It is not meant as an external API for
    interacting with files in DatastreamStorage.
    """

    def __init__(self, *args, **kwargs):
        super(AwsTemporaryStorage, self).__init__(*args, **kwargs)

        now = datetime.datetime.now()
        self._base_path = self.datastream_storage.temp_path.join(
            now.strftime("%Y-%m-%d.%H%M%S.%f")
        )

    @property
    def base_path(self) -> S3Path:
        return self._base_path

    def clean(self):
        super().clean()

        # Make sure all files under our temp folder are removed
        s3 = self.datastream_storage.s3_resource
        bucket = s3.Bucket(self.base_path.bucket_name)
        objects = bucket.objects.filter(Prefix=self.base_path.bucket_path)
        objects.delete()

    def is_tarfile(self, filepath):
        # We have to check based on filename not based upon opening the file,
        # so we can't use tarfile lib for this
        path = filepath.lower()
        return path.endswith(".tar") or path.endswith(".tar.gz")

    def is_zipfile(self, filepath):
        # We have to check based on filename not based upon opening the file,
        # so we can't use zipfile lib for this
        path = filepath.lower()
        return path.endswith(".zip")

    def extract_tarfile(self, filepath: S3Path) -> List[S3Path]:
        extracted_files = []
        s3_resource = self.datastream_storage.s3_resource
        tar_obj = s3_resource.Object(
            bucket_name=filepath.bucket_name, key=filepath.bucket_path
        )

        buffer = io.BytesIO(tar_obj.get()["Body"].read())
        tar = tarfile.open(fileobj=buffer, mode="r")

        for member in tar.getmembers():
            dest_path: S3Path = self.base_path.join(member.name)
            file_obj = tar.extractfile(member)

            if file_obj:  # file_obj will be None if it is a folder

                # do not include __MACOSX files
                if "__MACOSX" not in dest_path:
                    s3_resource.meta.client.upload_fileobj(
                        file_obj,
                        Bucket=dest_path.bucket_name,
                        Key=f"{dest_path.bucket_path}",
                    )
                    extracted_files.append(dest_path)

        tar.close()
        return extracted_files

    def extract_zipfile(self, filepath) -> List[S3Path]:
        s3_resource = self.datastream_storage.s3_resource
        zip_obj = s3_resource.Object(
            bucket_name=filepath.bucket_name, key=filepath.bucket_path
        )
        buffer = io.BytesIO(zip_obj.get()["Body"].read())
        extracted_files = []

        z = zipfile.ZipFile(buffer)
        for filename in z.namelist():
            dest_path: S3Path = self.base_path.join(filename)

            # do not include __MACOSX files
            if "__MACOSX" not in dest_path:
                s3_resource.meta.client.upload_fileobj(
                    z.open(filename),
                    Bucket=dest_path.bucket_name,
                    Key=f"{dest_path.bucket_path}",
                )
                extracted_files.append(dest_path)

        z.close()
        return extracted_files

    def extract_files(
        self, list_or_filepath: Union[S3Path, List[S3Path]]
    ) -> DisposableStorageTempFileList:

        extracted_files = []
        disposable_files = []

        files = list_or_filepath
        if isinstance(list_or_filepath, S3Path):
            files = [list_or_filepath]

        for filepath in files:
            if self.ignore_zip_check(filepath):
                is_tar = False
                is_zip = False
            else:
                is_tar = self.is_tarfile(filepath)  # .tar or .tar.gz
                is_zip = self.is_zipfile(filepath)  # .zip

            if is_tar or is_zip:
                # only dispose of the parent zip if set by storage policy
                if self.datastream_storage.remove_input_files:
                    disposable_files.append(filepath)

                if is_tar:
                    tmp_extracted_files = self.extract_tarfile(filepath)
                else:
                    tmp_extracted_files = self.extract_zipfile(filepath)

                # Add all the files that were extracted to the zip to the
                # list of returned files and mark them for auto-disposal
                extracted_files.extend(tmp_extracted_files)
                disposable_files.extend(tmp_extracted_files)

            else:
                # Only dispose of regular input files if set by storage policy.
                if self.datastream_storage.remove_input_files:
                    disposable_files.append(filepath)

                extracted_files.append(filepath)

        return DisposableStorageTempFileList(
            extracted_files, self, disposable_files=disposable_files
        )

    def fetch(
        self, file_path: S3Path, local_dir=None, disposable=True
    ) -> DisposableLocalTempFile:
        s3_client = self.datastream_storage.s3_client
        if not local_dir:
            local_dir = self.create_temp_dir()

        fetched_file = os.path.join(local_dir, os.path.basename(file_path.bucket_path))
        s3_client.download_file(
            file_path.bucket_name, file_path.bucket_path, fetched_file
        )

        if disposable:
            return DisposableLocalTempFile(fetched_file)

        return fetched_file

    def fetch_previous_file(
        self, datastream_name: str, start_time: str
    ) -> DisposableLocalTempFile:
        # fetch files one day previous and one day after start date (since find is exclusive)
        date = datetime.datetime.strptime(start_time, "%Y%m%d.%H%M%S")
        prev_date = (date - datetime.timedelta(days=1)).strftime("%Y%m%d.%H%M%S")
        next_date = (date + datetime.timedelta(days=1)).strftime("%Y%m%d.%H%M%S")
        files = self.datastream_storage.find(
            datastream_name,
            prev_date,
            next_date,
            filetype=DatastreamStorage.default_file_type,
        )

        previous_filepath = None
        if files:
            i = bisect.bisect_left(files, start_time)
            if i > 0:
                previous_filepath = files[i - 1]

        if previous_filepath:
            return self.fetch(previous_filepath)

        return DisposableLocalTempFile(None)

    def delete(self, filepath: S3Path) -> None:
        # First delete this resource.
        # (S3 will not crash if the file does not exist.)
        s3 = self.datastream_storage.s3_resource
        s3.Object(filepath.bucket_name, filepath.bucket_path).delete()

        # Then, delete any children if they exist.
        # S3 will not crash if there are no children.
        prefix = filepath.bucket_path
        prefix = f"{prefix}/" if not prefix.endswith("/") else prefix
        bucket = s3.Bucket(filepath.bucket_name)
        objects = bucket.objects.filter(Prefix=prefix)
        objects.delete()

    def listdir(self, filepath: S3Path) -> List[S3Path]:

        # List the files contained under this directory's s3 key.  This
        # This will only list the files under the given directory (not subfolders).
        # TODO: At some point we might need pagination to support cases where there
        #       are a huge number of files.  But this won't happen for a long time,
        #       if ever.

        paths = []
        s3_resource = self.datastream_storage.s3_resource
        bucket = s3_resource.Bucket(filepath.bucket_name)

        # Since we are using a delimiter, we must make sure the
        # prefix ends in /
        prefix = filepath.bucket_path
        prefix = f"{prefix}/" if not prefix.endswith("/") else prefix

        # Adding a delimeter of '/' will only get the files one level down
        for file in bucket.objects.filter(Prefix=prefix, Delimiter="/"):
            paths.append(S3Path(filepath.bucket_name, file.key))

        return paths

    def upload(self, local_path: str, s3_path: S3Path):

        s3_client = self.datastream_storage.s3_client

        with open(local_path, "rb") as f:
            s3_client.upload_fileobj(f, s3_path.bucket_name, s3_path.bucket_path)


class AwsStorage(DatastreamStorage):
    """DatastreamStorage subclass for an AWS S3-based filesystem.

    :param parameters: Dictionary of parameters that should be
        set automatically from the storage config file when this
        class is intantiated via the DatstreamStorage.from-config()
        method.  Defaults to {}

        Key parameters that should be set in the config file include

        :retain_input_files: Whether the input files should be cleaned
            up after they are done processing
        :root_dir: The bucket 'key' to use to prepend to all processed files
            created in the persistent store.  Defaults to 'root'
        :temp_dir: The bucket 'key' to use to prepend to all temp
            files created in the S3 bucket.  Defaults to 'temp'
        :bucket_name: The name of the S3 bucket to store to
    :type parameters: dict, optional
    """

    def __init__(self, parameters: Union[Dict, None] = None):
        parameters = parameters if parameters is not None else dict()
        super().__init__(parameters=parameters)
        bucket_name = self.parameters.get("bucket_name")
        storage_root_path = self.parameters.get("root_dir")
        storage_temp_path = self.parameters.get("temp_dir")

        assert bucket_name

        storage_root_path = "root" if not storage_root_path else storage_root_path
        storage_temp_path = "temp" if not storage_temp_path else storage_temp_path

        self._root = S3Path(bucket_name, storage_root_path)
        self._temp_path = S3Path(bucket_name, storage_temp_path)
        self._tmp = AwsTemporaryStorage(self)

        # Init the boto3 session so we can reuse it for all requests.
        # This will create a default session, which will pull the
        # region and the credentials from the local configuration.
        # Note that for now we assume that all buckets will be
        # in the same region where the lambda function is running.
        session = boto3.Session()
        self._s3_client = session.client("s3")
        self._s3_resource = session.resource("s3")

    @property
    def s3_resource(self):
        return self._s3_resource

    @property
    def s3_client(self):
        return self._s3_client

    @property
    def tmp(self):
        return self._tmp

    @property
    def root(self):
        return self._root

    @property
    def temp_path(self):
        return self._temp_path

    def find(
        self, datastream_name: str, start_time: str, end_time: str, filetype: str = None
    ) -> List[S3Path]:
        # TODO: think about refactoring so that you don't need both start and end time
        # TODO: if times don't include hours/min/sec, then add .000000 to the string
        subpath = DSUtil.get_datastream_directory(datastream_name=datastream_name)
        dir_to_check = self.root.join(subpath)
        storage_paths = []

        for file in self.tmp.listdir(dir_to_check):
            if start_time <= DSUtil.get_date_from_filename(file.bucket_path) < end_time:
                storage_paths.append(file)

        if filetype is not None:
            filter_func = DatastreamStorage.file_filters[filetype]
            storage_paths = list(filter(filter_func, storage_paths))

        return sorted(storage_paths)

    def fetch(
        self,
        datastream_name: str,
        start_time: str,
        end_time: str,
        local_path: str = None,
        filetype: int = None,
    ) -> DisposableLocalTempFileList:
        fetched_files = []
        datastream_store_files = self.find(
            datastream_name, start_time, end_time, filetype=filetype
        )
        local_dir = local_path
        if local_dir is None:
            local_dir = self.tmp.create_temp_dir()

        for datastream_file in datastream_store_files:
            fetched_file = self.tmp.fetch(
                datastream_file, local_dir=local_dir, disposable=False
            )
            fetched_files.append(fetched_file)

        return DisposableLocalTempFileList(fetched_files)

    def save_local_path(self, local_path: str, new_filename: str = None):
        # TODO: we should perform a REGEX check to make sure that the filename is valid
        filename = os.path.basename(local_path) if not new_filename else new_filename
        datastream_name = DSUtil.get_datastream_name_from_filename(filename)

        subpath = DSUtil.get_datastream_directory(datastream_name=datastream_name)
        s3_path = self.root.join(subpath, filename)

        self.tmp.upload(local_path, s3_path)
        return s3_path

    def exists(
        self, datastream_name: str, start_time: str, end_time: str, filetype: int = None
    ) -> bool:
        datastream_store_files = self.find(
            datastream_name, start_time, end_time, filetype=filetype
        )
        return len(datastream_store_files) > 0

    def delete(
        self, datastream_name: str, start_time: str, end_time: str, filetype: int = None
    ) -> None:
        files_to_delete = self.find(
            datastream_name, start_time, end_time, filetype=filetype
        )
        for file in files_to_delete:
            self.tmp.delete(file)
