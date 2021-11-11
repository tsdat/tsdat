import atexit
import bisect
import tarfile
import pathlib
import zipfile

import datetime
import _strptime  # noqa: F401
import os
import shutil
from typing import List, Union, Any, Dict

from . import (
    DatastreamStorage,
    TemporaryStorage,
    DisposableLocalTempFile,
    DisposableStorageTempFileList,
    DisposableLocalTempFileList,
)
from tsdat.utils import DSUtil


class FilesystemTemporaryStorage(TemporaryStorage):
    """Class used to store temporary files or perform
    fileystem actions on files other than datastream files
    that reside in the same local filesystem as the DatastreamStorage.
    This is a helper class intended to be used in the internals of
    pipeline implementations only.  It is not meant as an external API for
    interacting with files in DatastreamStorage.
    """

    def extract_files(
        self, list_or_filepath: Union[str, List[str]]
    ) -> DisposableStorageTempFileList:
        extracted_files = []
        disposable_files = []

        files = list_or_filepath
        if isinstance(list_or_filepath, str):
            files = [list_or_filepath]

        for filepath in files:
            is_file = os.path.isfile(filepath)

            if self.ignore_zip_check(filepath):
                is_tar = False
                is_zip = False
            else:
                is_tar = tarfile.is_tarfile(filepath)  # tar or tar.gz
                is_zip = zipfile.is_zipfile(filepath)

            if is_tar or is_zip:
                # only dispose of the parent zip if set by storage policy
                if self.datastream_storage.remove_input_files:
                    disposable_files.append(filepath)

                # Extract into a temporary folder in the target_dir
                temp_dir = self.create_temp_dir()

                if is_tar:
                    with tarfile.open(filepath) as tar:
                        tar.extractall(path=temp_dir)
                else:
                    with zipfile.ZipFile(filepath, "r") as zipped:
                        zipped.extractall(temp_dir)

                for filename in os.listdir(temp_dir):
                    filepath = os.path.join(temp_dir, filename)
                    if os.path.isfile(filepath):
                        extracted_files.append(filepath)

            elif is_file:
                # Only dispose of regular input files if set by storage policy.
                if self.datastream_storage.remove_input_files:
                    disposable_files.append(filepath)

                extracted_files.append(filepath)

        return DisposableStorageTempFileList(
            extracted_files, self, disposable_files=disposable_files
        )

    def fetch(
        self, file_path: str, local_dir=None, disposable=True
    ) -> Union[DisposableLocalTempFile, str]:
        if not local_dir:
            local_dir = self.create_temp_dir()

        fetched_file = os.path.join(local_dir, os.path.basename(file_path))
        shutil.copy(file_path, fetched_file)
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
        dates = [DSUtil.get_date_from_filename(_file) for _file in files]

        previous_filepath = None
        if dates:
            i = bisect.bisect_left(dates, start_time)
            if i > 0:
                previous_filepath = files[i - 1]

        if previous_filepath:
            return self.fetch(previous_filepath)

        return DisposableLocalTempFile(None)

    def delete(self, file_path: str) -> None:
        if os.path.isfile(file_path):
            os.remove(file_path)

        elif os.path.isdir(file_path):
            # remove directory and all its children
            shutil.rmtree(file_path)


class FilesystemStorage(DatastreamStorage):
    """Datastreamstorage subclass for a local Linux-based filesystem.

    TODO: rename to LocalStorage as this is more intuitive.

    :param parameters: Dictionary of parameters that should be
        set automatically from the storage config file when this
        class is intantiated via the DatstreamStorage.from-config()
        method.  Defaults to {}

        Key parameters that should be set in the config file include

        :retain_input_files: Whether the input files should be cleaned
            up after they are done processing
        :root_dir: The root path under which processed files will e stored.
    :type parameters: dict, optional
    """

    def __init__(self, parameters: Union[Dict, None] = None):
        parameters = parameters if parameters is not None else dict()
        super().__init__(parameters=parameters)

        self._root = parameters.get("root_dir")
        assert self._root

        # Make sure that the root folder exists
        pathlib.Path(self._root).mkdir(parents=True, exist_ok=True)

        self._tmp = FilesystemTemporaryStorage(self)

        # When this class is garbage collected, then make sure to
        # clean out the temp folders for any extraneous files
        atexit.register(self.tmp.clean)

    @property
    def tmp(self):
        return self._tmp

    def find(
        self, datastream_name: str, start_time: str, end_time: str, filetype: str = None
    ) -> List[str]:
        # TODO: think about refactoring so that you don't need both start and end time
        # TODO: if times don't include hours/min/sec, then add .000000 to the string
        dir_to_check = DSUtil.get_datastream_directory(
            datastream_name=datastream_name, root=self._root
        )
        storage_paths = []

        if os.path.isdir(dir_to_check):
            for file in os.listdir(dir_to_check):
                if start_time <= DSUtil.get_date_from_filename(file) < end_time:
                    storage_paths.append(os.path.join(dir_to_check, file))

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
            fetched_file = os.path.join(local_dir, os.path.basename(datastream_file))
            shutil.copy(datastream_file, fetched_file)
            fetched_files.append(fetched_file)

        return DisposableLocalTempFileList(fetched_files)

    def save_local_path(self, local_path: str, new_filename: str = None) -> Any:
        # TODO: we should perform a REGEX check to make sure that the filename is valid
        filename = os.path.basename(local_path) if not new_filename else new_filename
        datastream_name = DSUtil.get_datastream_name_from_filename(filename)

        dest_dir = DSUtil.get_datastream_directory(
            datastream_name=datastream_name, root=self._root
        )
        os.makedirs(dest_dir, exist_ok=True)  # make sure the dest folder exists
        dest_path = os.path.join(dest_dir, filename)

        shutil.copy(local_path, dest_path)
        return dest_path

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
            os.remove(file)
        return
