# TODO: Implement ZipReader
import pandas as pd
import xarray as xr
import re
import tarfile
from io import BytesIO
from zipfile import ZipFile
from pydantic import BaseModel, Extra
from typing import Any, Dict, List, Union
from .base import DataReader

__all__ = [
    "NetCDFReader",
    "CSVReader",
    "ParquetReader",
    "ZarrReader",
]


class NetCDFReader(DataReader):
    """---------------------------------------------------------------------------------
    Thin wrapper around xarray's `open_dataset()` function, with optional parameters
    used as keyword arguments in the function call.

    ---------------------------------------------------------------------------------"""

    parameters: Dict[str, Any] = {}

    def read(self, input_key: Union[str, BytesIO]) -> xr.Dataset:
        return xr.open_dataset(input_key, **self.parameters)  # type: ignore


class CSVReader(DataReader):
    """---------------------------------------------------------------------------------
    Uses pandas and xarray functions to read a csv file and extract its contents into an
    xarray Dataset object. Two parameters are supported: `read_csv_kwargs` and
    `from_dataframe_kwargs`, whose contents are passed as keyword arguments to
    `pandas.read_csv()` and `xarray.Dataset.from_dataframe()` respectively.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        read_csv_kwargs: Dict[str, Any] = {}
        from_dataframe_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: Union[str, BytesIO]) -> xr.Dataset:
        df: pd.DataFrame = pd.read_csv(input_key, **self.parameters.read_csv_kwargs)  # type: ignore
        # df.to_parquet()
        return xr.Dataset.from_dataframe(df, **self.parameters.from_dataframe_kwargs)


class ParquetReader(DataReader):
    """---------------------------------------------------------------------------------
    Uses pandas and xarray functions to read a parquet file and extract its contents
    into an xarray Dataset object. Two parameters are supported: `read_parquet_kwargs`
    and `from_dataframe_kwargs`, whose contents are passed as keyword arguments to
    `pandas.read_parquet()` and `xarray.Dataset.from_dataframe()` respectively.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        read_parquet_kwargs: Dict[str, Any] = {}
        from_dataframe_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: Union[str, BytesIO]) -> xr.Dataset:
        df: pd.DataFrame = pd.read_parquet(input_key, **self.parameters.read_parquet_kwargs)  # type: ignore
        return xr.Dataset.from_dataframe(df, **self.parameters.from_dataframe_kwargs)


class ZarrReader(DataReader):
    """---------------------------------------------------------------------------------
    Uses xarray's Zarr capabilities to read a Zarr archive and extract its contents into
    an xarray Dataset object.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        open_zarr_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: Union[str, BytesIO]) -> xr.Dataset:
        return xr.open_zarr(input_key, **self.parameters.open_zarr_kwargs)  # type: ignore


class ArchiveHandler(DataReader):
    """------------------------------------------------------------------------------------
    Base class for DataReader objects that read data from archives.

    This class, and any classes derived from it, require a `handlers` section in the
    parameters given to this `DataReader` from the storage configuration file. The
    structure of the `handlers` section underneath an `ArchiveHandler` should mirror the
    structure of its parent `handlers` section. To illustrate, consider the following
    configuration block for the `ZipHandler` class:

    .. code-block:: yaml

        file_handlers:
          input:
            zip:
              file_pattern: '.*\\.zip'
              classname: "tsdat.io.handlers.ZipHandler"
              parameters:
                # The handlers section tells the ArchiveHandler which DataReaders should be
                # used to handle the unpacked files.
                handlers:
                  csv:
                    file_pattern: '.*\\.csv'
                    classname: tsdat.io.handlers.CsvHandler
                    parameters:  # Parameters specific to tsdat.io.handlers.CsvHandler
                      sep: '\\t'
                # Pattern(s) used to exclude certain files in the archive from being handled. This
                # parameter is optional, and the default value is shown below:
                exclude: ['.*\\_\\_MACOSX/.*', '.*\\.DS_Store']

    All `ArchiveHandler` classes also allow an optional `exclude` parameter, which is used
    to filter out unpacked files matching regex patterns defined by the `exclude`
    parameter.

    Subclasses of `ArchiveHandler` may define additional parameters to support various
    methods of unpacking archived data.

    ------------------------------------------------------------------------------------"""

    _registry: HandlerRegistry = None
    _handlers: List[DataReader] = None
    """A list of DataReaders that the ArchiveHandler should use."""

    _exclude: str = None

    def __init__(self, parameters: Dict = None):
        super().__init__(parameters=parameters)
        self._registry = HandlerRegistry()
        self._handlers = list()

        for handler_dict in self.parameters["handlers"].values():
            child: DataReader = instantiate_handler(handler_desc=handler_dict)
            self._handlers.append(child)
            self._registry.register_file_handler(
                method="read", patterns=handler_dict["file_pattern"], handler=child,
            )

        # Naively merge a list of regex patterns to exclude certain files from being
        # read. By default we exclude files that macOS creates when zipping a folder.
        exclude = [".*\\_\\_MACOSX/.*", ".*\\.DS_Store"]
        exclude.extend(self.parameters.get("exclude", []))
        self._exclude = "(?:% s)" % "|".join(exclude)


class TarHandler(ArchiveHandler):
    """------------------------------------------------------------------------------------
    DataReader for reading from a tarred archive. Writing to this format is not supported.

    This class requires a that `handlers` be specified in the parameters section of the
    storage configuration file. The structure of the `handlers` section should mirror the
    structure of its parent `handlers` section. To illustrate, consider the following
    configuration block:

    .. code-block:: yaml

        file_handlers:
          input:
            tar:
              file_pattern: '.*\\.tar'
              classname: "tsdat.io.handlers.TarHandler"
              parameters:
                # Parameters to specify how the TarHandler should read/unpack the archive.
                read:
                  # Parameters here are passed to the Python open() method as kwargs. The
                  # default value is shown below.
                  open:
                    mode: "rb"

                  # Parameters here are passed to tarfile.open() as kwargs. Useful for
                  # specifying the system encoding or compression algorithm to use for
                  # unpacking the archive. These are optional.
                  # tarfile:
                    # mode: "r:gz"


                # The handlers section tells the TarHandler which DataReaders should be
                # used to handle the unpacked files.
                handlers:
                  csv:
                    file_pattern: '.*\\.csv'
                    classname: tsdat.io.handlers.CsvHandler
                    parameters:  # Parameters specific to tsdat.io.handlers.CsvHandler
                      sep: '\\t'

                # Pattern(s) used to exclude certain files in the archive from being handled.
                # This parameter is optional, and the default value is shown below:
                exclude: ['.*\\_\\_MACOSX/.*', '.*\\.DS_Store']

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        open_tar_kwargs: Dict[str, Any] = {}
        read_tar_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: Union[str, BytesIO]) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Extracts the file into memory and uses registered `DataReaders` to read each relevant
        extracted file into its own xarray Dataset object. Returns a mapping like
        {filename: xr.Dataset}.

        Args:
            file (Union[str, BytesIO]): The file to read in. Can be provided as a filepath or
            a bytes-like object. It is used to open the tar file.
            name (str, optional): A label used to help trace the origin of the data read-in.
            It is used in the key in the returned dictionary. Must be provided if the `file`
            argument is not string-like. If `file` is a string and `name` is not specified then
            the label will be set by `file`. Defaults to None.

        Returns:
            Dict[str, xr.Dataset]: A mapping of {label: xr.Dataset}.

        ------------------------------------------------------------------------------------"""
        # assert isinstance(input_key, str), "Must provide name if file is not a str."
        label = input_key
        output: Dict[str, xr.Dataset] = dict()

        # If we are reading from a string / filepath then add option to specify more
        # parameters for opening (i.e., mode or encoding options)
        if isinstance(input_key, str):
            open_params = dict(mode="rb")
            open_params.update(self.parameters.open_tar_kwargs)
            fileobj = open(input_key, **open_params)
        else:
            fileobj = input_key

        tar = tarfile.open(fileobj=fileobj, **self.parameters.read_tar_kwargs)
        for info_obj in tar:
            filename = info_obj.name
            file_label = f"{label}::{filename}"
            if re.match(self._exclude, filename):
                continue
            handler: DataReader = self._registry._get_handler(
                name=filename, method="read",
            )
            if handler:
                tar_bytes = BytesIO(tar.extractfile(filename).read())
                data = handler.read(input_key=tar_bytes, name=file_label)
                if isinstance(data, xr.Dataset):
                    data = {file_label: data}
                output.update(data)

        return output


class ZipHandler(ArchiveHandler):
    """------------------------------------------------------------------------------------
    DataReader for reading from a zipped archive. Writing to this format is not supported.

    This class requires a that `handlers` be specified in the parameters section of the
    storage configuration file. The structure of the `handlers` section should mirror the
    structure of its parent `handlers` section. To illustrate, consider the following
    configuration block:

    .. code-block:: yaml

        file_handlers:
          input:
            zip:
              file_pattern: '.*\\.zip'
              classname: "tsdat.io.handlers.ZipHandler"
              parameters:
                # Parameters to specify how the ZipHandler should read/unpack the archive.
                read:
                  # Parameters here are passed to the Python open() method as kwargs. The
                  # default value is shown below.
                  open:
                    mode: "rb"

                  # Parameters here are passed to zipfile.ZipFile.open() as kwargs. Useful
                  # for specifying the system encoding or compression algorithm to use for
                  # unpacking the archive. These are optional.
                  # zipfile:
                    # mode: "r"


                # The handlers section tells the ZipHandler which DataReaders should be
                # used to handle the unpacked files.
                handlers:
                  csv:
                    file_pattern: '.*\\.csv'
                    classname: tsdat.io.handlers.CsvHandler
                    parameters:  # Parameters specific to tsdat.io.handlers.CsvHandler
                      sep: '\\t'

                # Pattern(s) used to exclude certain files in the archive from being handled.
                # This parameter is optional, and the default value is shown below:
                exclude: ['.*\\_\\_MACOSX/.*', '.*\\.DS_Store']

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        open_zip_kwargs: Dict[str, Any] = {}
        read_zip_kwargs: Dict[str, Any] = {}

    parameters: Parameters = Parameters()

    def read(self, input_key: Union[str, BytesIO]) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Extracts the file into memory and uses registered `DataReaders` to read each relevant
        extracted file into its own xarray Dataset object. Returns a mapping like
        {filename: xr.Dataset}.

        Args:
            input_key (Union[str, BytesIO]): The file to read in. Can be provided as a filepath or
            a bytes-like object. It is used to open the zip file.
            name (str, optional): A label used to help trace the origin of the data read-in.
            It is used in the key in the returned dictionary. Must be provided if the `file`
            argument is not string-like. If `file` is a string and `name` is not specified then
            the label will be set by `file`. Defaults to None.

        Returns:
            Dict[str, xr.Dataset]: A mapping of {label: xr.Dataset}.

        ------------------------------------------------------------------------------------"""
        # assert isinstance(input_key, str), "Must provide name if file is not a str."

        label = input_key

        output: Dict[str, xr.Dataset] = dict()

        # If we are reading from a string / filepath then add option to specify more
        # parameters for opening (i.e., mode or encoding options)
        fileobj = None
        if isinstance(input_key, str):
            open_params = dict(mode="rb")
            open_params.update(self.parameters.open_zip_kwargs)
            fileobj = open(input_key, **open_params)
        else:
            fileobj = input_key

        zip = ZipFile(file=fileobj, **self.parameters.read_zip_kwargs)

        for filename in zip.namelist():
            file_label = f"{label}::{filename}"

            if re.match(self._exclude, filename):
                continue

            handler: DataReader = self._registry._get_handler(
                name=filename, method="read",
            )
            if handler:
                zip_bytes = BytesIO(zip.read(filename))
                data = handler.read(input_key=zip_bytes, name=file_label)

                if isinstance(data, xr.Dataset):
                    data = {file_label: data}

                output.update(data)

        return output
