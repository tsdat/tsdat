import boto3
import io
import os
import bisect
import datetime
import shutil
import tarfile
import yaml
import zipfile
from typing import List, Union

from tsdat.io import DatastreamStorage, \
    TemporaryStorage, \
    DisposableLocalTempFile, \
    DisposableStorageTempFileList, \
    DisposableLocalTempFileList

from tsdat.utils import DSUtil


SEPARATOR = '$$$'


class S3Path(str):
    """-------------------------------------------------------------------
    This class wraps a 'special' path string that lets us include the
    bucket name and region in the path, so that we can use it seamlessly
    in boto3 APIs.
    -------------------------------------------------------------------"""

    def __init__(self, bucket_name: str, bucket_path: str = '', region_name: str = None):
        self._bucket_name = bucket_name
        self._region_name = region_name

        # Note that os.path.join returns Windows separators if
        # running on Windows, which causes problems in S3,
        # so we are making sure the path uses linux line separators
        bucket_path = bucket_path.replace("\\", "/")

        # Make sure that bucket path does not start with a slash!

        self._bucket_path = bucket_path

    def __new__(cls, bucket_name: str, bucket_path: str, region_name: str = None):
        """-------------------------------------------------------------------
        We are creating our own string to hold the region, bucket & key (i.e., path),
        since boto3 needs all three in order to access a file

        Example:
        s3_client = boto3.client('s3', region_name='eu-central-1')
        s3_client.download_file(bucket, key, download_path)

        If region_name is not specified, then the default configured region is used.
        -------------------------------------------------------------------"""
        assert bucket_name
        assert bucket_path

        # Note that os.path.join returns Windows separators if
        # running on Windows, which causes problems in S3,
        # so we are making sure the path uses linux line separators
        bucket_path = bucket_path.replace("\\", "/")
        aws_path = ''

        if region_name:
            aws_path = f'{region_name}{SEPARATOR}{bucket_name}{SEPARATOR}{bucket_path}'
        else:
            aws_path = f'{bucket_name}{SEPARATOR}{bucket_path}'

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
        bucket_path = self.bucket_path

        for segment in args:
            bucket_path = os.path.join(bucket_path, segment)

        bucket_path = bucket_path.replace("\\", "/")
        return S3Path(self.bucket_name, bucket_path, self.region_name)


class AwsTemporaryStorage(TemporaryStorage):

    def __init__(self, *args, **kwargs):
        super(AwsTemporaryStorage, self).__init__(*args, **kwargs)

        now = datetime.datetime.now()
        self._base_path = self.datastream_storage.temp_path.join(now.strftime("%Y-%m-%d.%H%M%S.%f"))

    @property
    def base_path(self):
        return self._base_path

    def is_tarfile(self, filepath):
        # We have to check based on filename not based upon opening the file,
        # so we can't use tarfile lib for this
        path = filepath.lower()
        return path.endswith('.tar') or path.endswith('.tar.gz')

    def is_zipfile(self, filepath):
        # We have to check based on filename not based upon opening the file,
        # so we can't use zipfile lib for this
        path = filepath.lower()
        return path.endswith('.zip')

    def extract_tarfile(self, filepath: S3Path) -> List[S3Path]:
        extracted_files = []
        s3_resource = self.datastream_storage.s3_resource
        tar_obj = s3_resource.Object(bucket_name=filepath.bucket_name, key=filepath.bucket_path)

        buffer = io.BytesIO(tar_obj.get()["Body"].read())
        tar = tarfile.open(fileobj=buffer, mode='r')

        for member in tar.getmembers():
            dest_path: S3Path = self.base_path.join(member.name)
            file_obj = tar.extractfile(member)

            if file_obj:  # file_obj will be None if it is a folder

                # do not include __MACOSX files
                if '__MACOSX' not in dest_path:
                    s3_resource.meta.client.upload_fileobj(
                        file_obj,
                        Bucket=dest_path.bucket_name,
                        Key=f'{dest_path.bucket_path}'
                    )
                    extracted_files.append(dest_path)

        tar.close()
        return extracted_files

    def extract_zipfile(self, filepath) -> List[S3Path]:
        """-------------------------------------------------------------------
        Unzips the passed file from one S3 location into the temporary
        folder for this invocation (i.e., base_path) in memory without
        using local disk.

        Args:
            filepath (S3Path): The path to the zipfile in s3

        Returns:
            List[S3Path]: A list of the paths of files that were extracted
        -------------------------------------------------------------------"""
        s3_resource = self.datastream_storage.s3_resource
        zip_obj = s3_resource.Object(bucket_name=filepath.bucket_name, key=filepath.bucket_path)
        buffer = io.BytesIO(zip_obj.get()["Body"].read())
        extracted_files = []

        z = zipfile.ZipFile(buffer)
        for filename in z.namelist():
            dest_path: S3Path = self.base_path.join(filename)

            file_info = z.getinfo(filename)

            # do not include __MACOSX files
            if '__MACOSX' not in dest_path:
                s3_resource.meta.client.upload_fileobj(
                    z.open(filename),
                    Bucket=dest_path.bucket_name,
                    Key=f'{dest_path.bucket_path}'
                )
                extracted_files.append(dest_path)

        z.close()
        return extracted_files

    def extract_files(self, filepath: S3Path) -> DisposableStorageTempFileList:
        extracted_files = []
        delete_on_exception = True
        is_tar = self.is_tarfile(filepath)  # .tar or .tar.gz
        is_zip = self.is_zipfile(filepath)  # .zip

        if is_tar or is_zip:
            if is_tar:
                extracted_files = self.extract_tarfile(filepath)
            else:
                extracted_files = self.extract_zipfile(filepath)

        else:
            # If this is not a zip or tar file, we assume it is a regular file
            extracted_files.append(filepath)
            delete_on_exception = False

        return DisposableStorageTempFileList(extracted_files, self, delete_on_exception=delete_on_exception)

    def fetch(self, file_path: S3Path, local_dir=None, disposable=True) -> DisposableLocalTempFile:
        s3_client = self.datastream_storage.s3_client
        if not local_dir:
            local_dir = self.create_temp_dir()

        fetched_file = os.path.join(local_dir, os.path.basename(file_path.bucket_path))
        s3_client.download_file(file_path.bucket_name, file_path.bucket_path, fetched_file)

        if disposable:
            return DisposableLocalTempFile(fetched_file)

        return fetched_file

    def fetch_previous_file(self, datastream_name: str, start_time: str) -> DisposableLocalTempFile:
        # fetch files one day previous and one day after start date (since find is exclusive)
        date = datetime.datetime.strptime(start_time, "%Y%m%d.%H%M%S")
        prev_date = (date - datetime.timedelta(days=1)).strftime("%Y%m%d.%H%M%S")
        next_date = (date + datetime.timedelta(days=1)).strftime("%Y%m%d.%H%M%S")
        files = self.datastream_storage.find(datastream_name, prev_date, next_date, filetype=DatastreamStorage.FILE_TYPE.NETCDF)

        previous_filepath = None
        if files:
            i = bisect.bisect_left(files, start_time)
            if i > 0:
                previous_filepath = files[i-1]

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
        prefix = f'{prefix}/'if not prefix.endswith('/') else prefix
        bucket = s3.Bucket(filepath.bucket_name)
        objects = bucket.objects.filter(Prefix=prefix)
        objects.delete()

    def listdir(self, filepath: S3Path) -> List[S3Path]:
        """-----------------------------------------------------------------------
        List the files contained under this directory's s3 key.  This
        Will only list the files under the given directory (not subfolders).
        TODO: At some point we might need pagination to support cases where there
              are a huge number of files.  But this won't happen for a long time,
              if ever.
        -----------------------------------------------------------------------"""
        paths = []
        s3_resource = self.datastream_storage.s3_resource
        bucket = s3_resource.Bucket(filepath.bucket_name)

        # Since we are using a delimiter, we must make sure the
        # prefix ends in /
        prefix = filepath.bucket_path
        prefix = f'{prefix}/' if not prefix.endswith('/') else prefix

        # Adding a delimeter of '/' will only get the files one level down
        for file in bucket.objects.filter(Prefix=prefix, Delimiter='/'):
            paths.append(S3Path(filepath.bucket_name, file.key))

        return paths

    def upload(self, local_path: str, s3_path: S3Path):

        s3_client = self.datastream_storage.s3_client

        with open(local_path, "rb") as f:
            s3_client.upload_fileobj(f, s3_path.bucket_name, s3_path.bucket_path)


class AwsStorage(DatastreamStorage):

    """-----------------------------------------------------------------------
    DatastreamStorage subclass for an AWS S3-based filesystem.  See
    parent class for method docstrings.
    -----------------------------------------------------------------------"""

    def __init__(self, bucket_name: str = None,
                 storage_root_path: str = 'root',
                 storage_temp_path: str = 'temp'):
        """-------------------------------------------------------------------
        Initialize the storage from the given parameters used to connect
        to an S3 bucket.

        Args:
            bucket_name (str):  The name of the s3 bucket where the storage
                                files will be saved.

            storage_root_path (str): The path in the bucket to the root of the
                                     storage.

            storage_temp_path (str): The path in the bucket to a temporary
                                     folder used for writing short-lived temp
                                     files.
        -------------------------------------------------------------------"""
        assert bucket_name
        self._root = S3Path(bucket_name, storage_root_path)
        self._temp_path = S3Path(bucket_name, storage_temp_path)
        self._tmp = AwsTemporaryStorage(self)

        # Init the boto3 session so we can reuse it for all requests.
        # This will create a default session, which will pull the
        # region and the credentials from the local configuration.
        # Note that for now we assume that all buckets will be
        # in the same region where the lambda function is running.
        session = boto3.Session()
        self._s3_client = session.client('s3')
        self._s3_resource = session.resource('s3')

    @classmethod
    def from_config(cls, config_file):
        """-------------------------------------------------------------------
        Load a yaml config file which provides the storage constructor
        parameters.

        Args:
            config_file (str): The path to the config file to load

        Returns:
            AwsStorage: An AwsStorage instance created from the config file.
        -------------------------------------------------------------------"""
        dict = yaml.load(config_file, Loader=yaml.FullLoader)
        return AwsStorage(bucket_name=dict['bucket_name'],
                          storage_root_path=dict['storage_root_path'],
                          storage_temp_path=dict['storage_temp_path'])

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

    def find(self, datastream_name: str, start_time: str, end_time: str,
             filetype: int = None) -> List[S3Path]:
        # TODO: think about refactoring so that you don't need both start and end time
        # TODO: if times don't include hours/min/sec, then add .000000 to the string
        subpath = DSUtil.get_datastream_directory(datastream_name=datastream_name)
        dir_to_check = self.root.join(subpath)
        storage_paths = []

        for file in self.tmp.listdir(dir_to_check):
            if start_time <= DSUtil.get_date_from_filename(file.bucket_path) < end_time:
                storage_paths.append(file)

        if filetype == DatastreamStorage.FILE_TYPE.NETCDF:
            storage_paths = list(filter(lambda x: x.bucket_path.endswith('.nc'), storage_paths))

        elif filetype == DatastreamStorage.FILE_TYPE.PLOTS:
            storage_paths = list(filter(lambda x: DSUtil.is_image(x.bucket_path), storage_paths))

        elif filetype == DatastreamStorage.FILE_TYPE.RAW:
            storage_paths = list(filter(lambda x: '.raw.' in x.bucket_path, storage_paths))

        return sorted(storage_paths)

    def fetch(self, datastream_name: str, start_time: str, end_time: str,
              local_path: str = None,
              filetype: int = None) -> DisposableLocalTempFileList:
        fetched_files = []
        datastream_store_files = self.find(datastream_name, start_time, end_time, filetype=filetype)
        local_dir = local_path
        if local_dir is None:
            local_dir = self.tmp.create_temp_dir()

        for datastream_file in datastream_store_files:
            fetched_file = self.tmp.fetch(datastream_file, local_dir=local_dir, disposable=False)
            fetched_files.append(fetched_file)

        return DisposableLocalTempFileList(fetched_files)

    def save(self, local_path: str, new_filename: str = None) -> None:
        # TODO: we should perform a REGEX check to make sure that the filename is valid
        filename = os.path.basename(local_path) if not new_filename else new_filename
        datastream_name = DSUtil.get_datastream_name_from_filename(filename)

        subpath = DSUtil.get_datastream_directory(datastream_name=datastream_name)
        s3_path = self.root.join(subpath, filename)

        self.tmp.upload(local_path, s3_path)

    def exists(self, datastream_name: str, start_time: str, end_time: str, filetype: int = None) -> bool:
        datastream_store_files = self.find(datastream_name, start_time, end_time, filetype=filetype)
        return len(datastream_store_files) > 0

    def delete(self, datastream_name: str, start_time: str, end_time: str, filetype: int = None) -> None:
        files_to_delete = self.find(datastream_name, start_time, end_time, filetype=filetype)
        for file in files_to_delete:
            self.tmp.delete(file)



