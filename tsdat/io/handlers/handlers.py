import warnings
import re
import xarray as xr

from io import BytesIO
from typing import List, Dict, Literal, Union
from tsdat.config import Config


# TODO: Update the read methods to take file (str/BytesIO) and name (str). The file
# param will be used to open / read the data, and the name is used both to determine
# which handler in the registry to use and in individual handlers as a kind of label.
# For handlers, name should be optional if file is a str (can just set it as the file)
# and mandatory if file is a BytesIO or other object.


class DataHandler:
    """Abstract class to define methods required by all FileHandlers. Classes
    derived from `DataHandler` should implement a `read()` or `write()`
    method.

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

    def read(
        self,
        file: Union[str, BytesIO],
        name: str = None,
        **kwargs,
    ) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        """------------------------------------------------------------------------------------
        Reads the given file and converts it into either an xarray Dataset or mapping like
        `{label: xarray.Dataset}` for use in the pipeline.

        Args:
            file (Union[str, BytesIO]): The file to read in. Can be provided as a filepath or
            a bytes-like object.
            name (str, optional): A label used to help trace the origin of the data. Individual
            `DataHandlers` may choose not to use this, or may choose to make it mandatory.
            Defaults to None.

        Returns:
            Union[xr.Dataset, Dict[str, xr.Dataset]]: Returns either an xr.Dataset or a mapping
            like `{label: xr.Dataset}`, where `label` is typically a filename.

        ------------------------------------------------------------------------------------"""
        pass


class HandlerRegistry:
    """Class to provide methods to read and write files with a variety of
    extensions."""

    READERS: Dict[str, DataHandler] = None
    WRITERS: Dict[str, DataHandler] = None

    def __init__(self) -> None:
        self.READERS = dict()
        self.WRITERS = dict()

    def _get_handler(self, name: str, method: Literal["read", "write"]) -> DataHandler:
        """------------------------------------------------------------------------------------
        Given the filepath or name of the data to read or write and the method to apply to the
        data, this method determines which previously-registered DataHandler should be used.

        Args:
            name (str): The path to the file to read or write to, or a name that can be used to
            find the appropriate DataHandler.
            method (Literal["read", "write"]): The method to apply to the file.

        Returns:
            DataHandler: The DataHandler that should be applied.

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
            if regex.match(name):
                handler = handler_dict[pattern]
                break

        if handler is None:
            warnings.warn(f"No DataHandler found for: {name}")

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
        handler = self._get_handler(name=filename, method="write")
        if handler:
            handler.write(ds, filename, config, **kwargs)

    def read(
        self,
        file: Union[str, BytesIO],
        name: str = None,
        **kwargs,
    ) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        """------------------------------------------------------------------------------------
        Reads in the given file and converts it into an xarray dataset object using the
        registered FileHandler for the provided filepath.

        Args:
            filename (str): The path to the file to read in.

        Returns:
            xr.Dataset: The raw file as an Xarray.Dataset object.

        ------------------------------------------------------------------------------------"""

        assert name or isinstance(file, str), "name is required if file is not a str"

        label: str = name if name else file

        handler = self._get_handler(label, "read")
        if handler:
            return handler.read(file=file, name=name, **kwargs)

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
