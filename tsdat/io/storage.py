import os
import abc
import shutil
import tempfile
import zipfile
import tarfile
import datetime
import boto3
import xarray as xr
from typing import List, Dict


class LocalTempFile:
    """-------------------------------------------------------------------
    TemporaryFile is a context manager wrapper class for a temp file on
    the local filesystem.  It will ensure that the file is deleted when
    it goes out of scope.
    -------------------------------------------------------------------"""
    def __init__(self, filepath: str):
        self.filepath = filepath

    def __enter__(self):
        return self.filepath

    def __exit__(self, type, value, traceback):
        os.remove(self.filepath)


class DatastreamStorage(abc.ABC):

    @property
    def _tmp(self):
        """-------------------------------------------------------------------
        Each subclass should define the _tmp property, which provides
        access to a TemporaryStorage object that is used to efficiently
        handle reading writing temporary files used during the processing
        pipeline.  Is is not intended to be used outside of the pipeline.
        -------------------------------------------------------------------"""
        raise NotImplementedError

    @abc.abstractmethod
    def fetch(self, datastream_name: str, start_time: str, end_time: str, local_path: str = None) -> List[str]:
        """-------------------------------------------------------------------
        Fetches a file from the datastream store using the datastream_name, 
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

        Returns:
            List[str]:  A list of paths where the retrieved files were stored
                        in local storage.  
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
    def exists(self, datastream_name: str, start_time: str, end_time: str) -> bool:
        """-------------------------------------------------------------------
        Checks if data exists in the datastream store for the provided 
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

        Returns:
            bool: True if data exists, False otherwise.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def delete(self, datastream_name: str, start_time: str, end_time: str) -> None:
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
        -------------------------------------------------------------------"""
        return


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

    @property
    def local_temp_folder(self):
        """-------------------------------------------------------------------
        Default method to get a local temporary folder for use when retrieving
        files from temporary storage.  This method should work for all
        filesystems, but can be overridden if needed by subclasses.

        Returns:
            List[str]:         A list of paths unique to the storage
                               filesystem where the files were extracted.
        -------------------------------------------------------------------"""
        if self._local_temp_folder is None:
            tempfile.mkdtemp()

        return self._local_temp_folder

    @abc.abstractmethod
    def unzip(self, file_path: str) -> List:
        """-------------------------------------------------------------------
        Extract a file into a temp directory in the same filesystem as
        the storage.  This is for efficient processing when working in a
        cloud environment.

        Args:
            file_path (str):   The path of a file located in the same
                               filesystem as the storage.

        Returns:
            List[str]:         A list of paths unique to the storage
                               filesystem where the files were extracted.
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def fetch(self, file_path: str) -> LocalTempFile:
        """-------------------------------------------------------------------
        Fetch a file from temp storage to a local temp folder

        Args:
            file_path (str):   The path of a file located in the same
                               filesystem as the storage.

        Returns:
            LocalTempFile:     The local path to the file
        -------------------------------------------------------------------"""
        pass

    @abc.abstractmethod
    def fetch_previous_file(self, datastream_name: str, start_date: str, start_time: str) -> LocalTempFile:
        """-------------------------------------------------------------------
        Look in DatastreamStorage for the first file before the given date.

        Args:
            datastream_name (str):
            start_date (str):
            start_time (str):

        Returns:
            LocalTempFile:          The local path to the file
        -------------------------------------------------------------------"""
        pass



