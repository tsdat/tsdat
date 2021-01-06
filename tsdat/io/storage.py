import os
import abc
import zipfile
import datetime
from typing import List

class DatastreamStorage(abc.ABC):

    @abc.abstractmethod
    def fetch(self, datastream_name: str, start_time: str, end_time: str, local_path: str) -> List[str]:
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
    def save(self, local_path: str) -> None:
        """-------------------------------------------------------------------
        Saves a local file to the datastream store.

        Args:
            local_path (str):   The path to the local file to save. The file 
                                should be named according to MHKiT-Cloud 
                                naming conventions.
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


class FilesystemStorage(DatastreamStorage):
    """-----------------------------------------------------------------------
    DatastreamStorage subclass for a typical Linux-based filesystem.
    -----------------------------------------------------------------------"""

    def __init__(self, root: str):
      self.__root = root