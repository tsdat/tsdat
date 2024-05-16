from typing import Optional
import re
import tarfile
from io import BytesIO
from typing import Any, Dict, List

import xarray as xr
from pydantic import BaseModel, Extra

from ..base import ArchiveReader, DataReader


class TarReader(ArchiveReader):
    """DataReader for reading from a tarred archive. Writing to this format is not
    supported.

    This class requires a that `readers be specified in the parameters section of the
    storage configuration file. The structure of the `readers section should mirror the
    structure of its parent `readers section. To illustrate, consider the following
    configuration block:

    ```yaml
    readers:
      .*:
        tar:
          file_pattern: .*\.tar
          classname: tsdat.io.readers.TarReader
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
              .*\.csv:
                classname: tsdat.io.readers.CSVReader
                parameters:  # Parameters specific to tsdat.io.readers.CSVReader
                  read_csv_kwargs:
                    sep: '\\t'

            # Pattern(s) used to exclude certain files in the archive from being handled.
            # This parameter is optional, and the default value is shown below:
            exclude: ['.*\_\_MACOSX/.*', '.*\.DS_Store']
    ```

    """

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
            input_key (str): The file to read in. It is used to open the tar file.

        Returns:
            Dict[str, xr.Dataset]: A mapping of {label: xr.Dataset}.

        ------------------------------------------------------------------------------------
        """

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
                reader: Optional[DataReader] = self.parameters.readers.get(key, None)
                if reader:
                    tar_bytes = BytesIO(tar.extractfile(filename).read())  # type: ignore
                    data = reader.read(tar_bytes)  # type: ignore

                    if isinstance(data, xr.Dataset):
                        data = {filename: data}  # type: ignore
                    output.update(data)  # type: ignore

        return output
