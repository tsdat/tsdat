import re
import tarfile
import xarray as xr

from io import BytesIO
from typing import Dict, List, Union
from tsdat.config.utils import instantiate_handler
from zipfile import ZipFile
from .handlers import DataHandler, HandlerRegistry


class ArchiveHandler(DataHandler):

    registry: HandlerRegistry = None
    handlers: List[DataHandler] = None
    exclude: str = None

    def __init__(self, parameters: Dict = None):
        # TODO: Validate parameters
        super().__init__(parameters=parameters)

        self.registry = HandlerRegistry()

        self.handlers = list()
        for handler_dict in self.parameters["handlers"].values():
            child: DataHandler = instantiate_handler(handler_desc=handler_dict)
            self.handlers.append(child)
            self.registry.register_file_handler(
                method="read",
                patterns=handler_dict["file_pattern"],
                handler=child,
            )

        # Naively merge a list of regex patterns to exclude certain files from being
        # read. By default we exclude files that macOS creates when zipping a folder.
        exclude = [".*\\_\\_MACOSX/.*", ".*\\.DS_Store"]
        exclude.extend(self.parameters.get("exclude", []))
        self.exclude = "(?:% s)" % "|".join(exclude)


class TarHandler(ArchiveHandler):
    def read(
        self,
        file: Union[str, BytesIO],
        name: str = None,
        **kwargs,
    ) -> Dict[str, xr.Dataset]:
        """------------------------------------------------------------------------------------
        Extracts the file into memory and uses registered `DataHandlers` to read each relevant
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
        assert name or isinstance(file, str), "Must provide name if file is not a str."

        label = name if name else file

        output: Dict[str, xr.Dataset] = dict()

        read_params: Dict = self.parameters.get("read", {})

        # If we are reading from a string / filepath then add option to specify more
        # parameters for opening (i.e., mode or encoding options)
        fileobj = None
        if isinstance(file, str):
            open_params = dict(mode="rb")
            open_params.update(read_params.get("open", {}))
            fileobj = open(file, **open_params)
        else:
            fileobj = file

        tarfile_params = read_params.get("tarfile", {})
        tar = tarfile.open(fileobj=fileobj, **tarfile_params)

        for info_obj in tar:
            filename = info_obj.name

            file_label = f"{label}::{filename}"

            if re.match(self.exclude, filename):
                continue

            handler: DataHandler = self.registry._get_handler(
                name=filename,
                method="read",
            )
            if handler:

                tar_bytes = BytesIO(tar.extractfile(filename).read())
                data = handler.read(file=tar_bytes, name=file_label)

                if isinstance(data, xr.Dataset):
                    data = {file_label: data}

                output.update(data)

        return output


class ZipHandler(ArchiveHandler):
    def read(
        self,
        file: Union[str, BytesIO],
        name: str = None,
        **kwargs,
    ) -> Dict[str, xr.Dataset]:
        """------------------------------------------------------------------------------------
        Extracts the file into memory and uses registered `DataHandlers` to read each relevant
        extracted file into its own xarray Dataset object. Returns a mapping like
        {filename: xr.Dataset}.

        Args:
            file (Union[str, BytesIO]): The file to read in. Can be provided as a filepath or
            a bytes-like object. It is used to open the zip file.
            name (str, optional): A label used to help trace the origin of the data read-in.
            It is used in the key in the returned dictionary. Must be provided if the `file`
            argument is not string-like. If `file` is a string and `name` is not specified then
            the label will be set by `file`. Defaults to None.

        Returns:
            Dict[str, xr.Dataset]: A mapping of {label: xr.Dataset}.

        ------------------------------------------------------------------------------------"""
        assert name or isinstance(file, str), "Must provide name if file is not a str."

        label = name if name else file

        output: Dict[str, xr.Dataset] = dict()

        read_params = self.parameters.get("read", {})

        # If we are reading from a string / filepath then add option to specify more
        # parameters for opening (i.e., mode or encoding options)
        fileobj = None
        if isinstance(file, str):
            open_params = dict(mode="rb")
            open_params.update(read_params.get("open", {}))
            fileobj = open(file, **open_params)
        else:
            fileobj = file

        zipfile_params = read_params.get("zipfile", {})
        zip = ZipFile(file=fileobj, **zipfile_params)

        for filename in zip.namelist():
            file_label = f"{label}::{filename}"

            if re.match(self.exclude, filename):
                continue

            handler: DataHandler = self.registry._get_handler(
                name=filename,
                method="read",
            )
            if handler:
                zip_bytes = BytesIO(zip.read(filename))
                data = handler.read(file=zip_bytes, name=file_label)

                if isinstance(data, xr.Dataset):
                    data = {file_label: data}

                output.update(data)

        return output
