import pandas as pd
import xarray as xr
import re
import tarfile
from io import BytesIO
from zipfile import ZipFile
from pydantic import BaseModel, Extra
from typing import Any, Dict, List
from .base import DataReader, ArchiveReader

__all__ = [
    "NetCDFReader",
    "CSVReader",
    "ParquetReader",
    "ZarrReader",
    "ZipReader",
]


class NetCDFReader(DataReader):
    """---------------------------------------------------------------------------------
    Thin wrapper around xarray's `open_dataset()` function, with optional parameters
    used as keyword arguments in the function call.

    ---------------------------------------------------------------------------------"""

    parameters: Dict[str, Any] = {}

    def read(self, input_key: str) -> xr.Dataset:
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

    def read(self, input_key: str) -> xr.Dataset:
        df: pd.DataFrame = pd.read_csv(input_key, **self.parameters.read_csv_kwargs)  # type: ignore
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

    def read(self, input_key: str) -> xr.Dataset:
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

    def read(self, input_key: str) -> xr.Dataset:
        return xr.open_zarr(input_key, **self.parameters.open_zarr_kwargs)  # type: ignore


class TarReader(ArchiveReader):
    """------------------------------------------------------------------------------------
    DataReader for reading from a tarred archive. Writing to this format is not supported.

    This class requires a that `readers be specified in the parameters section of the
    storage configuration file. The structure of the `readers section should mirror the
    structure of its parent `readers section. To illustrate, consider the following
    configuration block:

    .. code-block:: yaml

        readers:
          .*:
            tar:
              file_pattern: '.*\\.tar'
              classname: "tsdat.io.readers.TarReader"
              parameters:
                # Parameters to specify how the TarReader should read/unpack the archive.
                # Parameters here are passed to the Python open() method as kwargs. The
                # default value is shown below.
                open_tar_kwargs:
                  mode: "rb"

                # Parameters here are passed to tarfile.open() as kwargs. Useful for
                # specifying the system encoding or compression algorithm to use for
                # unpacking the archive. These are optional.
                read_tar_kwargs:
                  mode: "r:gz"


                # The readers section tells the TarReader which DataReaders should be
                # used to handle the unpacked files.
                readers:
                  r".*\\.csv":
                    classname: tsdat.io.readers.CSVReader
                    parameters:  # Parameters specific to tsdat.io.readers.CSVReader
                      read_csv_kwargs:
                        sep: '\\t'

                # Pattern(s) used to exclude certain files in the archive from being handled.
                # This parameter is optional, and the default value is shown below:
                exclude: ['.*\\_\\_MACOSX/.*', '.*\\.DS_Store']

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        open_tar_kwargs: Dict[str, Any] = {}
        read_tar_kwargs: Dict[str, Any] = {}
        readers: Dict[str, Any] = {}
        exclude: List[str] = []

    parameters: Parameters = Parameters()

    def read(self, input_key: str) -> Dict[str, xr.Dataset]:
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

        output: Dict[str, xr.Dataset] = {}

        # If we are reading from a string / filepath then add option to specify more
        # parameters for opening (i.e., mode or encoding options)
        if isinstance(input_key, str):  # Necessary for archiveReaders
            open_params = dict(mode="rb")
            open_params.update(self.parameters.open_tar_kwargs)
            fileobj = open(input_key, **open_params)  # type: ignore
        else:
            fileobj = input_key

        tar = tarfile.open(fileobj=fileobj, **self.parameters.read_tar_kwargs)  # type: ignore

        for info_obj in tar:  # type: ignore
            filename = info_obj.name  # type: ignore
            if re.match(self.parameters.exclude, filename):  # type: ignore
                continue

            for key in self.parameters.readers.keys():
                reader: DataReader = self.parameters.readers.get(key, None)
                if reader:
                    tar_bytes = BytesIO(tar.extractfile(filename).read())  # type: ignore
                    data = reader.read(tar_bytes)  # type: ignore

                    if isinstance(data, xr.Dataset):
                        data = {filename: data}  # type: ignore
                    output.update(data)  # type: ignore

        return output


class ZipReader(ArchiveReader):
    """------------------------------------------------------------------------------------
    DataReader for reading from a zipped archive. Writing to this format is not supported.

    This class requires a that `readers be specified in the parameters section of the
    storage configuration file. The structure of the `readers section should mirror the
    structure of its parent `readers section. To illustrate, consider the following
    configuration block:

    .. code-block:: yaml

        readers:
          .*:
            zip:
              file_pattern: '.*\\.zip'
              classname: "tsdat.io.readers.ZipReader"
              parameters:
                # Parameters to specify how the ZipReader should read/unpack the archive.
                # Parameters here are passed to the Python open() method as kwargs. The
                # default value is shown below.
                open_zip_kwargs:
                  mode: "rb"

                # Parameters here are passed to zipfile.ZipFile.open() as kwargs. Useful
                # for specifying the system encoding or compression algorithm to use for
                # unpacking the archive. These are optional.
                read_zip_kwargs:
                  mode: "r"


                # The readers section tells the ZipReaders which DataReaders should be
                # used to read the unpacked files.
                readers:
                  r".*\\.csv":
                    classname: tsdat.io.readers.CSVReader
                    parameters:  # Parameters specific to tsdat.io.readers.CsvReader
                        read_csv_kwargs:
                        sep: '\\t'

                # Pattern(s) used to exclude certain files in the archive from being handled.
                # This parameter is optional, and the default value is shown below:
                exclude: ['.*\\_\\_MACOSX/.*', '.*\\.DS_Store']

    ------------------------------------------------------------------------------------"""

    class Parameters(BaseModel, extra=Extra.forbid):
        open_zip_kwargs: Dict[str, Any] = {}
        read_zip_kwargs: Dict[str, Any] = {}
        readers: Dict[str, Any] = {}
        exclude: List[str] = []

    parameters: Parameters = Parameters()

    def read(self, input_key: str) -> Dict[str, xr.Dataset]:
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
        output: Dict[str, xr.Dataset] = {}

        # If we are reading from a string / filepath then add option to specify more
        # parameters for opening (i.e., mode or encoding options)
        fileobj = None
        if isinstance(input_key, str):  # Necessary for archiveReaders
            open_params = dict(mode="rb")
            open_params.update(self.parameters.open_zip_kwargs)
            fileobj = open(input_key, **open_params)  # type: ignore
        else:
            fileobj = input_key

        zip = ZipFile(file=fileobj, **self.parameters.read_zip_kwargs)  # type: ignore

        for filename in zip.namelist():
            if re.match(self.parameters.exclude, filename):  # type: ignore
                continue

            for key in self.parameters.readers.keys():
                reader: DataReader = self.parameters.readers.get(key, None)
                if reader:
                    zip_bytes = BytesIO(zip.read(filename))
                    data = reader.read(zip_bytes)  # type: ignore

                    if isinstance(data, xr.Dataset):
                        data = {filename: data}
                    output.update(data)

        return output
