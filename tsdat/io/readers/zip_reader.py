import re
from io import BytesIO
from typing import Any, Dict, List, Optional
from zipfile import ZipFile

import xarray as xr
from pydantic import BaseModel, Extra

from ..base import ArchiveReader, DataReader


class ZipReader(ArchiveReader):
    """DataReader for reading from a zipped archive. Writing to this format is not
    supported.

    This class requires a that `readers be specified in the parameters section of the
    storage configuration file. The structure of the `readers section should mirror the
    structure of its parent `readers section. To illustrate, consider the following
    configuration block:

    ```yaml
    readers:
      .*:
        zip:
          file_pattern: .*\.zip
          classname: tsdat.io.readers.ZipReader
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
              .*\.csv:
                classname: tsdat.io.readers.CSVReader
                parameters:  # Parameters specific to tsdat.io.readers.CsvReader
                    read_csv_kwargs:
                    sep: '\\t'

            # Pattern(s) used to exclude certain files in the archive from being handled.
            # This parameter is optional, and the default value is shown below:
            exclude: ['.*\_\_MACOSX/.*', '.*\.DS_Store']
    ```

    """

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

        ------------------------------------------------------------------------------------
        """
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

        zip_file = ZipFile(file=fileobj, **self.parameters.read_zip_kwargs)  # type: ignore

        for filename in zip_file.namelist():
            if re.match(self.parameters.exclude, filename):  # type: ignore
                continue

            for key in self.parameters.readers.keys():
                if not re.match(key, filename):
                    continue

                reader: Optional[DataReader] = self.parameters.readers.get(key, None)
                if reader:
                    zip_bytes = BytesIO(zip_file.read(filename))
                    data = reader.read(zip_bytes)  # type: ignore

                    if isinstance(data, xr.Dataset):
                        data = {filename: data}
                    output.update(data)

        return output
