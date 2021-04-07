import warnings

import abc
import functools
import os
import re
import xarray as xr
from typing import List, Dict

from tsdat.config import Config


# TODO: add a file handler for parquet format

class AbstractFileHandler(abc.ABC):
    """-----------------------------------------------------------------------
    Abstract class to define methods required by all FileHandlers. Classes 
    derived from AbstractFileHandler must implement the following methods:
    ```
    write(ds: xr.Dataset, filename: str, config: Config, **kwargs)
    read(filename: str, **kwargs) -> xr.Dataset
    ```
    -----------------------------------------------------------------------"""

    def __init__(self, parameters={}):
        self.parameters = parameters

    def write(self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs) -> None:
        """-------------------------------------------------------------------
        Classes derived from the FileHandler class must implement this method.

        Saves the given dataset to file.

        Args:
            ds (xr.Dataset): The dataset to save.
            filename (str): An absolute or relative path to the file including
                            filename.
            config (Config, optional):  Optional Config object. Defaults to 
                                        None.
        -------------------------------------------------------------------"""
        #TODO: we can derive file name from the dataset - just need to specify the destination folder
        # Return the path to the local file that was saved
        pass

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """-------------------------------------------------------------------
        Classes derived from the FileHandler class must implement this method.
        
        This method reads the given file into a xr.Dataset object.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: A xr.Dataset object
        -------------------------------------------------------------------"""
        pass


class FileHandler:
    """-----------------------------------------------------------------------
    Class to provided methods to read and write files with a variety of 
    extensions.
    -----------------------------------------------------------------------"""
    FILEHANDLERS: Dict[str, AbstractFileHandler] = {}
    
    @staticmethod
    def _get_handler(filename: str) -> AbstractFileHandler:
        handler = None

        # Iterate through the file handlers, applying their
        # regex to see if the filename is a match.
        for pattern in FileHandler.FILEHANDLERS.keys():
            regex = re.compile(pattern)
            if regex.match(filename):
                handler = FileHandler.FILEHANDLERS[pattern]
                break

        if handler is None:
            warnings.warn(f"No FileHandler found for file: {filename}")

        return handler

    @staticmethod
    def write(ds: xr.Dataset, filename: str, config: Config = None, **kwargs) -> None:
        """-------------------------------------------------------------------
        Saves the given dataset to file using the registered FileHandler for 
        the filename's extension and optional keyword arguments.

        Args:
            ds (xr.Dataset): The dataset to save.
            filename (str): An absolute or relative path to the file including
                            filename.
            config (Config, optional):  Optional Config object. Defaults to 
                                        None.
        -------------------------------------------------------------------"""
        handler = FileHandler._get_handler(filename)
        if handler:
            handler.write(ds, filename, config, **kwargs)

    @staticmethod
    def read(filename: str, **kwargs) -> xr.Dataset:
        """-------------------------------------------------------------------     
        This method reads the given file into a xr.Dataset object using the 
        registered FileHandler for the filename's extension.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: A xr.Dataset object
        -------------------------------------------------------------------"""
        handler = FileHandler._get_handler(filename)
        if handler:
            return handler.read(filename, **kwargs)

    @staticmethod
    def register_file_handler(patterns: str, handler: AbstractFileHandler):
        if isinstance(patterns, List):
            for pattern in patterns:
                FileHandler.FILEHANDLERS[pattern] = handler
        else:
            FileHandler.FILEHANDLERS[patterns] = handler


def register_filehandler(pattern: str):
    """-----------------------------------------------------------------------
    Python decorator to register a class in the FILEHANDLERS dictionary. This
    dictionary will be used by the MHKiT-Cloud pipeline to read and write raw,
    intermediate, and processed data.

    This decorator can be used to work with a specific file handler without
    having to specify a config file.  This is useful when using the file handler
    for analysis or for tests.  For pipelines, handlers should always be
    specified via the storage config file.

    Example Usage:
    ```
    @register_filehandler(["*.nc", "*.cdf"])
    class NetCdfHandler(AbstractFileHandler):
        def write(self, dataset, filename):
            pass
        def read(self, filename, config):
            pass
    ```

    Args:
        pattern (str | List):    The regex file name pattern that this
                                 FileHandler should be registered for.
    -----------------------------------------------------------------------"""
    def decorator_register(cls):
        FileHandler.register_file_handler(pattern, cls)
        @functools.wraps(cls)
        def wrapper_register(*args, **kwargs):
            return cls(*args, **kwargs)
        return wrapper_register
    return decorator_register