import warnings
import functools
import re
import xarray as xr
from typing import List, Dict, Union
from tsdat.config import Config


class AbstractFileHandler:
    """Abstract class to define methods required by all FileHandlers. Classes
    derived from AbstractFileHandler should implement one or more of the 
    following methods:
    
    ``write(ds: xr.Dataset, filename: str, config: Config, **kwargs)``

    ``read(filename: str, **kwargs) -> xr.Dataset``

    :param parameters: 
        Parameters that were passed to the FileHandler when it was 
        registered in the storage config file, defaults to {}.
    :type parameters: Dict, optional
    """

    def __init__(self, parameters: Dict = {}):
        self.parameters = parameters

    def write(self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs) -> None:
        """Saves the given dataset to a file. 

        :param ds: The dataset to save.
        :type ds: xr.Dataset
        :param filename: The path to where the file should be written to.
        :type filename: str
        :param config: Optional Config object, defaults to None
        :type config: Config, optional
        """
        pass

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """Reads in the given file and converts it into an Xarray dataset for
        use in the pipeline. 

        :param filename: The path to the file to read in.
        :type filename: str
        :return: A xr.Dataset object.
        :rtype: xr.Dataset
        """
        pass


class FileHandler:
    """Class to provide methods to read and write files with a variety of 
    extensions."""

    FILEHANDLERS: Dict[str, AbstractFileHandler] = {}
    
    @staticmethod
    def _get_handler(filename: str) -> AbstractFileHandler:
        """Given the name of the file to read or write, this method applies
        a regular expression to match the name of the file with a handler that
        has been registered in its internal dictionary of FileHandler objects
        and returns the appropriate FileHandler, or None if a match is not 
        found.

        :param filename: 
            The name of the file whose handler should be retrieved.
        :type filename: str
        :return: 
            The FileHandler registered for use with the provided filename.
        :rtype: AbstractFileHandler
        """
        handler = None

        # Iterate through the file handlers, applying their regex to see if 
        # the filename is a match.
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
        """Saves the given dataset to file using the registered FileHandler 
        for the provided filename.

        :param ds: The dataset ot save.
        :type ds: xr.Dataset
        :param filename: The path to where the file should be written to.
        :type filename: str
        :param config: Optional Config object, defaults to None
        :type config: Config, optional
        """
        handler = FileHandler._get_handler(filename)
        if handler:
            handler.write(ds, filename, config, **kwargs)

    @staticmethod
    def read(filename: str, **kwargs) -> xr.Dataset:
        """Reads in the given file and converts it into an Xarray dataset 
        using the registered FileHandler for the provided filename. 

        :param filename: The path to the file to read in.
        :type filename: str
        :return: A xr.Dataset object.
        :rtype: xr.Dataset
        """
        handler = FileHandler._get_handler(filename)
        if handler:
            return handler.read(filename, **kwargs)

    @staticmethod
    def register_file_handler(patterns: Union[str, List[str]], handler: AbstractFileHandler):
        """Static method to register an AbstractFileHandler for one or more 
        file patterns. Once an AbstractFileHandler has been registered it may 
        be used by this class to read or write files whose paths match one or 
        more pattern(s) provided in registration.

        :param patterns: 
            The patterns (regex) that should be used to match a filepath to 
            the AbstractFileHandler provided. 
        :type patterns: Union[str, List[str]]
        :param handler: The AbstractFileHandler to register.
        :type handler: AbstractFileHandler
        """
        if isinstance(patterns, List):
            for pattern in patterns:
                FileHandler.FILEHANDLERS[pattern] = handler
        else:
            FileHandler.FILEHANDLERS[patterns] = handler


def register_filehandler(patterns: Union[str, List[str]]) -> AbstractFileHandler:
    """Python decorator to register an AbstractFileHandler in the FileHandler
    object. The FileHandler object will be used by tsdat pipelines to read and
    write raw, intermediate, and processed data.

    This decorator can be used to work with a specific AbstractFileHandler 
    without having to specify a config file. This is useful when using an
    AbstractFileHandler for analysis or for tests outside of a pipeline. For 
    tsdat pipelines, handlers should always be specified via the storage 
    config file.

    Example Usage:

    .. code-block:: python
        
        import xarray as xr
        from tsdat.io import register_filehandler, AbstractFileHandler

        @register_filehandler(["*.nc", "*.cdf"])
        class NetCdfHandler(AbstractFileHandler):
            def write(ds: xr.Dataset, filename: str, config: Config = None, **kwargs):
                ds.to_netcdf(filename)
            def read(filename: str, **kwargs) -> xr.Dataset:
                xr.load_dataset(filename)
    :param patterns: 
        The patterns (regex) that should be used to match a filepath to 
        the AbstractFileHandler provided. 
    :type patterns: Union[str, List[str]]
    :return: 
        The original AbstractFileHandler class, after it has been registered 
        for use in tsdat pipelines.
    :rtype: AbstractFileHandler
    """
    def decorator_register(cls):
        FileHandler.register_file_handler(patterns, cls)
        @functools.wraps(cls)
        def wrapper_register(*args, **kwargs):
            return cls(*args, **kwargs)
        return wrapper_register
    return decorator_register
