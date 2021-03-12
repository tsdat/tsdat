import abc
import os
import shutil
import tempfile
from datetime import datetime
from typing import List, Union


class DatastreamStorage(abc.ABC):

    # TODO: we need a way to take the output file handlers that are
    # registered in the config file and dynamically add them to a
    # dict of filters so that users can specify a file type key, and
    # then be able to correctly filter data files in storage that
    # map to that type.  Plot and raw types should be automatically
    # added to the dictionary.
    class FILE_TYPE():
        NETCDF = 1
        PLOTS = 2
        RAW = 3

    @property
    def tmp(self):
        """-------------------------------------------------------------------
        Each subclass should define the tmp property, which provides
        access to a TemporaryStorage object that is used to efficiently
        handle reading writing temporary files used during the processing
        pipeline.  Is is not intended to be used outside of the pipeline.
        -------------------------------------------------------------------"""
        raise NotImplementedError

    @abc.abstractmethod
    def find(self, datastream_name: str, start_time: str, end_time: str,
             filetype: int = None) -> List[str]:
        """-------------------------------------------------------------------
        Finds all files of the given type from the datastream store with the
        given datastream_name and timestamps from start_time (inclusive) up to
        end_time (exclusive).  Returns a list of paths to files that match the
        criteria.

        Args:
            datastream_name (str):  The datastream_name as defined by
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106.000000" to
                                search for data beginning on or after
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108.000000" to search
                            for data ending before January 8th, 2021.

            filetype (int): A file type from the DatastreamStorage.FILE_TYPE
                            list.  If no type is specified, all files will
                            be returned.

        Returns:
            List[str]:  A list of paths in datastream storage in ascending order
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def fetch(self, datastream_name: str, start_time: str, end_time: str,
              local_path: str = None, filetype: int = None) -> List[str]:
        """-------------------------------------------------------------------
        Fetches files from the datastream store using the datastream_name,
        start_time, and end_time to specify the file(s) to retrieve. If the 
        local path is not specified, it is up to the subclass to determine
        where to put the retrieved file(s).

        Args:
            datastream_name (str):  The datastream_name as defined by 
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106" to
                                search for data beginning on or after 
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108" to search
                            for data ending before January 8th, 2021.
            local_path (str):   The path to the directory where the data
                                should be stored.
            filetype (int):   A file type from the DatastreamStorage.FILE_TYPE
                              list.  If no type is specified, all files will
                              be returned.

        Returns:
            DisposableStorageTempFileList:  A list of paths where
                                the retrieved files were stored in local storage.
        -------------------------------------------------------------------"""
        return
    
    @abc.abstractmethod
    def save(self, local_path: str, new_filename: str = None) -> None:
        """-------------------------------------------------------------------
        Saves a local file to the datastream store.

        Args:
            local_path (str):   The local path to the file to save. The file 
                                should be named according to MHKiT-Cloud 
                                naming conventions so that this method can
                                automatically parse the datastream, date,
                                and time from the file name.
            new_filename (str): If provided, the new filename to save as. 
                                Must also follow MHKIT-Cloud naming 
                                conventions.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def exists(self, datastream_name: str, start_time: str, end_time: str,
               filetype: int = None) -> bool:
        """-------------------------------------------------------------------
        Checks if any data exists in the datastream store for the provided
        datastream and time range.

        Args:
            datastream_name (str):  The datastream_name as defined by 
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106" to
                                search for data beginning on or after 
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108" to search
                            for data ending before January 8th, 2021.
            filetype (int):  A file type from the DatastreamStorage.FILE_TYPE
                             list.  If no type is specified, all files will
                             be returned.

        Returns:
            bool: True if data exists, False otherwise.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def delete(self, datastream_name: str, start_time: str, end_time: str,
               filetype: int = None) -> None:
        """-------------------------------------------------------------------
        Deletes datastream data in the datastream store in between the 
        specified time range. 

        Args:
            datastream_name (str):  The datastream_name as defined by 
                                    MHKiT-Cloud Data Standards.
            start_time (str):   The start time or date to start searching for
                                data (inclusive). Should be like "20210106" to
                                search for data beginning on or after 
                                January 6th, 2021.
            end_time (str): The end time or date to stop searching for data
                            (exclusive). Should be like "20210108" to search
                            for data ending before January 8th, 2021.
            filetype (int):  A file type from the DatastreamStorage.FILE_TYPE
                             list.  If no type is specified, all files will
                             be returned.
        -------------------------------------------------------------------"""
        return


class DisposableLocalTempFile:
    """-------------------------------------------------------------------
    DisposableLocalTempFile is a context manager wrapper class for a temp file on
    the LOCAL FILESYSTEM.  It will ensure that the file is deleted when
    it goes out of scope.
    -------------------------------------------------------------------"""
    def __init__(self, filepath: str, disposable=True):
        self.filepath = filepath
        self.disposable = disposable

    def __enter__(self):
        return self.filepath

    def __exit__(self, type, value, traceback):

        # We only clean up the file if an exception was not thrown
        if type is None and self.filepath is not None and self.disposable:
            if os.path.isfile(self.filepath):
                os.remove(self.filepath)

            elif os.path.isdir(self.filepath):
                # remove directory and all its children
                shutil.rmtree(self.filepath)


class DisposableLocalTempFileList (list):
    """-------------------------------------------------------------------
    Provides a context manager wrapper class for a list of
    temp files on the LOCAL FILESYSTEM.  It will ensure that the files
    are deleted when the list goes out of scope.
    -------------------------------------------------------------------"""

    def __init__(self, filepath_list: List[str], delete_on_exception=False, disposable=True):
        """-------------------------------------------------------------------
        Args:
            filepath_list (List[str]):   A list of paths to files in temporary
                                         storage.

            delete_on_exception:        The default behavior is to not remove
                                        the files on exit if an exception
                                        occurs.  However, users can override this
                                        setting to force files to be cleaned up
                                        no matter if an exception is thrown or
                                        not.
            disposable:                 True if this file should be auto-deleted
                                        when out of scope.
        -------------------------------------------------------------------"""
        self.filepath_list = filepath_list
        self.delete_on_exception = delete_on_exception
        self.disposable = disposable

    def __enter__(self):
        return self.filepath_list

    def __exit__(self, type, value, traceback):

        if self.disposable:
            # We only clean up the files if an exception was not thrown
            if type is None or self.delete_on_exception:
                for filepath in self.filepath_list:
                    if os.path.isfile(filepath):
                        os.remove(filepath)

                    elif os.path.isdir(filepath):
                        # remove directory and all its children
                        shutil.rmtree(filepath)


class DisposableStorageTempFileList (list):
    """-------------------------------------------------------------------
    Provides is a context manager wrapper class for a list of
    temp files on the STORAGE FILESYSTEM.  It will ensure that the files
    are deleted when the list goes out of scope.
    -------------------------------------------------------------------"""

    def __init__(self, filepath_list: List[str], storage, delete_on_exception=False, disposable=True):
        """-------------------------------------------------------------------
        Args:
            filepath_list (List[str]):   A list of paths to files in temporary
                                         storage.

            storage (TemporaryStorage): The temporary storage service used
                                        to clean up temporary files.

            delete_on_exception:        The default behavior is to not remove
                                        the files on exit if an exception
                                        occurs.  However, users can override this
                                        setting to force files to be cleaned up
                                        no matter if an exception is thrown or
                                        not.

            disposable:                 True if this file should be auto-deleted
                                        when out of scope.
        -------------------------------------------------------------------"""
        self.filepath_list = filepath_list

        # Make sure that we have passed the right class
        if isinstance(storage, DatastreamStorage):
            storage = storage.tmp
        self.tmp_storage = storage
        assert isinstance(self.tmp_storage, TemporaryStorage)
        self.delete_on_exception = delete_on_exception
        self.disposable = disposable

    def __enter__(self):
        return self.filepath_list

    def __exit__(self, type, value, traceback):

        if self.disposable:
            # We only clean up the files if an exception was not thrown
            if type is None or self.delete_on_exception:
                for filepath in self.filepath_list:
                    self.tmp_storage.delete(filepath)


class TemporaryStorage(abc.ABC):
    """-------------------------------------------------------------------
    TemporaryStorage is used to efficiently handle reading writing temporary
    files used during the processing pipeline.  TemporaryStorage methods
    return a context manager so that the created temporary files can be
    automatically removed when they go out of scope.
    -------------------------------------------------------------------"""

    def __init__(self, storage: DatastreamStorage):
        """-------------------------------------------------------------------
        Args:
            storage (DatastreamStorage): A reference to the corresponding
                                         DatastreamStorage
        -------------------------------------------------------------------"""
        self.datastream_storage = storage
        self._local_temp_folder = tempfile.mkdtemp(prefix='tsdat-pipeline-')

    @property
    def local_temp_folder(self) -> str:
        """-------------------------------------------------------------------
        Default method to get a local temporary folder for use when retrieving
        files from temporary storage.  This method should work for all
        filesystems, but can be overridden if needed by subclasses.

        Returns:
            str:   Path to local temp folder
        -------------------------------------------------------------------"""
        return self._local_temp_folder

    def clean(self):
        # remove any garbage files left in the local temp folder
        shutil.rmtree(self.local_temp_folder)

    def get_temp_filepath(self, filename: str = None, disposable: bool = True) -> DisposableLocalTempFile:
        """-------------------------------------------------------------------
        Construct a filepath for a temporary file that will be located in the
        storage-approved local temp folder and will be deleted when it goes
        out of scope.

        Args:
            filename (str):   The filename to use for the temp file.  If no
                              filename is provided, one will be created.

            disposable (bool): If true, then wrap in DisposableLocalTempfile so
                               that the file will be removed when it goes out of
                               scope

        Returns:
            DisposableLocalTempFile:   Path to the local file.  The file will be
                                       automatically deleted when it goes out
                                       of scope.
        -------------------------------------------------------------------"""
        if filename is None:
            now = datetime.now()
            filename = now.strftime("%Y-%m-%d.%H%M%S.%f")

        filepath = os.path.join(self.local_temp_folder, filename)
        if disposable:
            return DisposableLocalTempFile(filepath)
        else:
            return filepath

    def create_temp_dir(self) -> str:
        """-------------------------------------------------------------------
        Create a new, temporary directory under the local tmp area managed by
        TemporaryStorage.

        Returns:
            str:   Path to the local dir.
        -------------------------------------------------------------------"""
        now = datetime.now()
        filename = now.strftime("%Y-%m-%d.%H%M%S.%f")
        temp_dir = os.path.join(self.local_temp_folder, filename)

        # make sure the directory exists
        os.makedirs(temp_dir, exist_ok=False)

        return temp_dir

    @abc.abstractmethod
    def extract_files(self, file_path: str) -> DisposableStorageTempFileList:
        """-------------------------------------------------------------------
        If provided a path to an archive file, this function will extract the
        archive into a temp directory IN THE SAME FILESYSTEM AS THE STORAGE.
        This means, for example that if storage was in an s3 bucket ,then
        the files would be extracted to a temp dir in that s3 bucket.  This
        is to prevent local disk limitations when running via Lambda.

        If the file is not an archive, then the same file will be returned.

        This method supports zip, tar, and tar.g file formats.

        Args:
            file_path (str):   The path of a file located in the same
                               filesystem as the storage.

        Returns:
            DisposableStorageTempFileList:  A list of paths to the files that were extracted.
                                  Files will be located in the temp area of the
                                  storage filesystem.
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def fetch(self, file_path: str, local_dir=None, disposable=True) -> Union[DisposableLocalTempFile, str]:
        """-------------------------------------------------------------------
        Fetch a file from temp storage to a local temp folder.  If
        disposable is True, then a DisposableLocalTempFile will be returned
        so that it can be used with a context manager.

        Args:
            file_path (str):   The path of a file located in the same
                               filesystem as the storage.
            local_dir(str):    The destination folder for the file.  If not
                               specified, it will be created.
            disposable (bool):

        Returns:
            DisposableLocalTempFile | str:   The local path to the file
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def fetch_previous_file(self, datastream_name: str, start_time) -> DisposableLocalTempFile:
        """-------------------------------------------------------------------
        Look in DatastreamStorage for the first file before the given date.

        Args:
            datastream_name (str):
            start_time (str):

        Returns:
            DisposableLocalTempFile:          The local path to the file
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def delete(self, file_path: str) -> None:
        """-------------------------------------------------------------------
        Remove a file from storage temp area if the file exists.  If the file
        does not exists, this method will NOT raise an exception.

        Args:
            file_path (str):   The path of a file located in the same
                               filesystem as the storage.
        -------------------------------------------------------------------"""
        pass

