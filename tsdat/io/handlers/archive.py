import tarfile
import xarray as xr

from io import BytesIO
from pathlib import Path
from tsdat.config.utils import instantiate_handler
from zipfile import ZipFile
from .handlers import DataHandler, HandlerRegistry

from typing import Dict, List, Union


class ArchiveHandler(DataHandler):

    registry: HandlerRegistry = None
    handlers: List[DataHandler] = None

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


class TarHandler(ArchiveHandler):
    def read(self, filename: str, **kwargs) -> xr.Dataset:
        return xr.Dataset


class ZipHandler(ArchiveHandler):
    def read(self, file: Union[Path, BytesIO], **kwargs) -> Dict[str, xr.Dataset]:

        output: Dict[str, xr.Dataset] = dict()

        zipfile_params = self.parameters.get("read", {}).get("zipfile", {})
        zip = ZipFile(file, **zipfile_params)

        for filename in zip.namelist():
            handler = self.registry._get_handler(filename=filename, method="read")
            if handler:
                # TODO: let handlers take filename and/or bytes. Here we are hacking
                # the DataHandlers by providing bytes as the filename. This will
                # inevitably fail when someone writes a handler that uses the filename
                # to open a file or do string manipulations with it.
                bytes = BytesIO(zip.read(filename))
                data = handler.read(bytes)

                # Convert the output to a mapping so it can be combined.
                if isinstance(data, xr.Dataset):
                    data = {filename: data}

                # Prepend the keys with the filename (zip file). This is only needed
                # until handlers are modified to take bytes and/or filename, as they
                # can define the mapping appropriately at that point.
                elif isinstance(data, Dict):
                    data: Dict[str, xr.Dataset] = {
                        f"{filename}:{k}": v for k, v in data.items()
                    }

                else:
                    raise ValueError(f"Unexpected output from {handler}: {data}")

                output.update(data)

        return output
