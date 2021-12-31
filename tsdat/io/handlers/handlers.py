import warnings
import re
import xarray as xr
from typing import List, Dict, Literal, Union
from tsdat.config import Config


class DataHandler:
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

    def __init__(self, parameters: Union[Dict, None] = None):
        self.parameters = parameters if parameters is not None else dict()

    def write(
        self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs
    ) -> None:
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


class HandlerRegistry:
    """Class to provide methods to read and write files with a variety of
    extensions."""

    READERS: Dict[str, DataHandler] = None
    WRITERS: Dict[str, DataHandler] = None

    def __init__(self) -> None:
        self.READERS = dict()
        self.WRITERS = dict()

    def _get_handler(
        self, filename: str, method: Literal["read", "write"]
    ) -> DataHandler:
        """------------------------------------------------------------------------------------
        Given the filepath of the file to read or write and the FileHandler method to apply to
        the filepath, this method determines which previously-registered FileHandler should be
        used on the provided filepath.

        Args:
            filename (str): The path to the file to read or write to.
            method (Literal["read", "write"]): The method to apply to the file.

        Returns:
            AbstractFileHandler: The FileHandler that should be applied.

        ------------------------------------------------------------------------------------"""
        assert method in ["read", "write"]

        handler_dict = self.READERS
        if method == "write":
            handler_dict = self.WRITERS

        handler = None

        # Iterate through the file handlers, applying their regex to see if the
        # filename is a match.
        for pattern in handler_dict.keys():
            regex = re.compile(pattern)
            if regex.match(filename):
                handler = handler_dict[pattern]
                break

        if handler is None:
            warnings.warn(f"No FileHandler found for file: {filename}")

        return handler

    def write(
        self, ds: xr.Dataset, filename: str, config: Config = None, **kwargs
    ) -> None:
        """------------------------------------------------------------------------------------
        Calls the appropriate FileHandler to write the dataset to the provided filename.

        Args:
            ds (xr.Dataset): The dataset to save.
            filename (str): The path to the file where the dataset should be written.
            config (Config, optional): Optional Config object. Defaults to None.

        ------------------------------------------------------------------------------------"""
        handler = self._get_handler(filename, "write")
        if handler:
            handler.write(ds, filename, config, **kwargs)

    def read(self, filename: str, **kwargs) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Reads in the given file and converts it into an xarray dataset object using the
        registered FileHandler for the provided filepath.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: The raw file as an Xarray.Dataset object.

        ------------------------------------------------------------------------------------"""
        handler = self._get_handler(filename, "read")
        if handler:
            return handler.read(filename, **kwargs)

    def register_file_handler(
        self,
        method: Literal["read", "write"],
        patterns: Union[str, List[str]],
        handler: DataHandler,
    ):
        """------------------------------------------------------------------------------------
        Method to register a FileHandler for reading from or writing to files matching one or
        more provided file patterns.

        Args:
            method ("Literal"): The method the FileHandler should call if the pattern is
            matched. Must be one of: "read", "write".
            patterns (Union[str, List[str]]): The file pattern(s) that determine if this
            FileHandler should be run on a given filepath.
            handler (AbstractFileHandler): The AbstractFileHandler to register.

        ------------------------------------------------------------------------------------"""
        assert method in ["read", "write"]

        handler_dict = self.READERS
        if method == "write":
            handler_dict = self.WRITERS

        if isinstance(patterns, str):
            patterns = [patterns]

        for pattern in patterns:
            handler_dict[pattern] = handler
