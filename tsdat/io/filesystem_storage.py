import os
import shutil
import tarfile
import zipfile
from typing import List, Union

from tsdat.io import DatastreamStorage, \
    TemporaryStorage, \
    DisposableLocalTempFile, \
    DisposableStorageTempFileList, \
    DisposableLocalTempFileList

from tsdat.utils import DSUtil


class FilesystemTemporaryStorage(TemporaryStorage):

    def extract_files(self, filepath: str) -> DisposableStorageTempFileList:
        extracted_files = []
        delete_on_exception = True
        is_tar = tarfile.is_tarfile(filepath) # tar or tar.gz
        is_zip = zipfile.is_zipfile(filepath)

        if is_tar or is_zip:
            # Extract into a temporary folder in the target_dir
            temp_dir = self.create_temp_dir()

            if is_tar:
                with tarfile.open(filepath) as tar:
                    tar.extractall(path=temp_dir)
            else:
                with zipfile.ZipFile(filepath, 'r') as zipped:
                    zipped.extractall(temp_dir)

            for filename in os.listdir(temp_dir):
                extracted_files.append(os.path.join(temp_dir, filename))

        else:
            # If this is not a zip or tar file, we assume it is a regular file
            extracted_files.append(filepath)
            delete_on_exception = False

        return DisposableStorageTempFileList(extracted_files, self, delete_on_exception=delete_on_exception)

    def fetch(self, file_path: str) -> DisposableLocalTempFile:
        return DisposableLocalTempFile(file_path)

    def fetch_previous_file(self, datastream_name: str, start_date: str, start_time: str) -> DisposableLocalTempFile:
        pass

    def delete(self, file_path: str) -> None:
        if os.path.isfile(file_path):
            os.remove(file_path)

        elif os.path.isdir(file_path):
            # remove directory and all its children
            shutil.rmtree(file_path)


class FilesystemStorage(DatastreamStorage):

    """-----------------------------------------------------------------------
    DatastreamStorage subclass for a typical Linux-based filesystem.  See
    parent class for method docstrings.
    -----------------------------------------------------------------------"""

    def __init__(self, root: str = ""):
        self._root = root
        self._tmp = FilesystemTemporaryStorage(self)

    @property
    def tmp(self):
        return self._tmp

    def find(self, datastream_name: str, start_time: str, end_time: str,
             filetype: int = None) -> List[str]:
        dir_to_check = DSUtil.get_datastream_directory(datastream_name=datastream_name, root=self._root)
        storage_paths = []

        if os.path.isdir(dir_to_check):
            for file in os.listdir(dir_to_check):
                if start_time <= DSUtil.get_date_from_filename(file) < end_time:
                    storage_paths.append(os.path.join(dir_to_check, file))

            if filetype == DatastreamStorage.FILE_TYPE.NETCDF:
                storage_paths = list(filter(lambda x: x.endswith('.nc'), storage_paths))

            elif filetype == DatastreamStorage.FILE_TYPE.PLOTS:
                storage_paths = list(filter(lambda x: DSUtil.is_image(x), storage_paths))

            elif filetype == DatastreamStorage.FILE_TYPE.RAW:
                storage_paths = list(filter(lambda x: '.raw.' in x, storage_paths))

        return storage_paths

    def fetch(self, datastream_name: str, start_time: str, end_time: str,
              local_path: str = None,
              filetype: int = None,
              disposable=True) -> Union[List[str], DisposableLocalTempFileList]:
        fetched_files = []
        datastream_store_files = self.find(datastream_name, start_time, end_time, filetype=filetype)
        local_dir = local_path
        if local_dir is None:
            local_dir = self.tmp.create_temp_dir()

        for datastream_file in datastream_store_files:
            fetched_file = os.path.join(local_dir, os.path.basename(datastream_file))
            shutil.copy(datastream_file, fetched_file)
            fetched_files.append(fetched_file)

        if disposable:
            return DisposableLocalTempFileList(fetched_files)

        return fetched_files

    def save(self, local_path: str, new_filename: str = None) -> None:
        # TODO: we should perform a REGEX check to make sure that the
        # filename is valid
        filename = os.path.basename(local_path) if not new_filename else new_filename
        datastream_name = DSUtil.get_datastream_name_from_filename(filename)

        dest_dir = DSUtil.get_datastream_directory(datastream_name=datastream_name, root=self._root)
        os.makedirs(dest_dir, exist_ok=True)  # make sure the dest folder exists
        dest_path = os.path.join(dest_dir, filename)

        shutil.copy(local_path, dest_path)

    def exists(self, datastream_name: str, start_time: str, end_time: str, filetype: int = None) -> bool:
        datastream_store_files = self.find(datastream_name, start_time, end_time, filetype=filetype)
        return len(datastream_store_files) > 0

    def delete(self, datastream_name: str, start_time: str, end_time: str, filetype: int = None) -> None:
        files_to_delete = self.find(datastream_name, start_time, end_time, filetype=filetype)
        for file in files_to_delete:
            os.remove(file)
        return

