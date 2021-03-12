import os
import abc
import yaml
import functools
import warnings
import numpy as np
import pandas as pd
import xarray as xr
from typing import List, Dict
from tsdat.config import Config
from tsdat.utils import DSUtil

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

    @staticmethod
    def write(ds: xr.Dataset, filename: str, config: Config = None, **kwargs) -> None:
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
    
    @staticmethod
    def read(filename: str, **kwargs) -> xr.Dataset:
        """-------------------------------------------------------------------
        Classes derived from the FileHandler class must implement this method.
        
        This method reads the given file into a xr.Dataset object.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: A xr.Dataset object
        -------------------------------------------------------------------"""
        pass


class FileHandler():
    """-----------------------------------------------------------------------
    Class to provided methods to read and write files with a variety of 
    extensions.
    -----------------------------------------------------------------------"""
    FILEHANDLERS: Dict[str, AbstractFileHandler] = {}
    
    @staticmethod
    def _get_handler(filename: str) -> AbstractFileHandler:
        _, ext = os.path.splitext(filename)
        if ext not in FileHandler.FILEHANDLERS:
            # raise KeyError(f"No FileHandler has been registered for extension: {ext}")
            warnings.warn(f"No FileHandler has been registered for extension: {ext}")
            return None
        return FileHandler.FILEHANDLERS[ext]

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


# TODO: register by regex instead of file extension
# TODO: Make this a static class function and have the decorator wrapper
# call it.
# Then users can call this independently to use a file handler or the
# storage can use it to register all handlers defined in their config
def register_filehandler(file_extension: str):
    """-----------------------------------------------------------------------
    Python decorator to register a class in the FILEHANDLERS dictionary. This
    dictionary will be used by the MHKiT-Cloud pipeline to read and write raw,
    intermediate, and processed data.

    Example Usage:
    ```
    @register_filehandler([".nc", ".cdf"])
    class NetCdfHandler(AbstractFileHandler):
        def write(self, dataset, filename):
            pass
        def read(self, filename, config):
            pass
    ```

    Args:
        file_extension (str | List):    The file extension(s) that this 
                                        FileHandler should be registered for.
    -----------------------------------------------------------------------"""
    def decorator_register(cls):
        if isinstance(file_extension, List):
            for ext in file_extension:
                FileHandler.FILEHANDLERS[ext] = cls
        else:
            FileHandler.FILEHANDLERS[file_extension] = cls
        @functools.wraps(cls)
        def wrapper_register(*args, **kwargs):
            return cls(*args, **kwargs)
        return wrapper_register
    return decorator_register