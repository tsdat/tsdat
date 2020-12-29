import os
import abc
import zipfile
from typing import List, Tuple

class DatastreamStorage(abc.ABC):

    @abc.abstractmethod
    def fetch(self, datastream_store_path: str, local_path: str) -> str:
        """-------------------------------------------------------------------
        Fetches a file from the datastream store. If the local path is not 
        specified, it is up to the subclass to determine where to put the 
        retrieved file.

        Args:
            datastream_store_path (str):    The path to the file in the 
                                            datastream storage to fetch.
            local_path (str):   The path to where the retrieved file should 
                                be saved.

        Returns:
            str: Returns the path to the retrieved file in local storage.
        -------------------------------------------------------------------"""
        return
    
    @abc.abstractmethod
    def save(self, local_path: str, datastream_store_path: str) -> None:
        """-------------------------------------------------------------------
        Saves a local file to the datastream store.

        Args:
            local_path (str): The path to the local file to save.
            datastream_store_path (str):    The path in the datastream store 
                                            where the file should be saved to.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def exists(self, datastream_store_path: str) -> bool:
        """-------------------------------------------------------------------
        Checks if the file exists in the datastream store

        Args:
            datastream_store_path (str): The path to the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def delete(self, datastream_store_path: str) -> None:
        """-------------------------------------------------------------------
        Deletes a file in the datastream store

        Args:
            datastream_store_path (str): The path to the file to delete.
        -------------------------------------------------------------------"""
        return

    @abc.abstractmethod
    def listdir(self, datastream_store_path: str) -> Tuple[List[str], List[str]]: # might not be needed
        """-------------------------------------------------------------------
        Lists the files and directories in the specified path and returns a 
        2-tuple of lists where the first item is directories and the second 
        item is files. 

        Args:
            datastream_store_path (str):    The directory whose contents 
                                            should be listed.

        Returns:
            Tuple[List[str], List[str]]:    A 2-tuple with directories as the 
                                            first item and files paths as the
                                            second item.
        -------------------------------------------------------------------"""
        return
    
    @abc.abstractmethod
    def rename(self, previous_datastream_store_path: str, new_datastream_store_path: str) -> None:
        """-------------------------------------------------------------------
        Renames a file in the datastream store.

        Args:
            previous_datastream_store_path (str):   The path to an existing 
                                                    file to be renamed.
            new_datastream_store_path (str): The desired new path to the file. 
        -------------------------------------------------------------------"""
        return


class FilesystemStorage(DatastreamStorage):
    """-----------------------------------------------------------------------
    DatastreamStorage subclass for a typical Linux-based filesystem.
    -----------------------------------------------------------------------"""

    def __init__(self, root):
      self.__root = root     

