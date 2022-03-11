import xarray as xr
from datetime import datetime
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Any
from pydantic import BaseModel, Extra
from tsdat.io.handlers.handlers import HandlerRegistry


class BaseStorage(BaseModel, ABC, extra=Extra.forbid):

    parameters: Any = {}
    registry: HandlerRegistry

    @abstractmethod
    def save_data(self, dataset: xr.Dataset):
        ...

    @abstractmethod
    def find_data(self, datastream: str, start: datetime, end: datetime):
        ...

    @abstractmethod
    def fetch_data(self, datastream: str, start: datetime, end: datetime):
        ...

    @abstractmethod
    def delete_data(self, datastream: str, start: datetime, end: datetime):
        # TODO: Is this needed?
        ...

    @abstractmethod
    def save_ancillary_file(self, datastream: str, file: Path):
        ...

    # IDEA: Context manager for temporary / ancillary files. More of a utility storage
    # method than something critically necessary, but would still be used in pipelines.
    # @abstractmethod
    # def upload_directory_contents(self, dirpath: Path):
    #     ...

    # @contextlib.contextmanager
    # def uploadable_tmp_dir(self, upload: bool = True) -> Generator[Path, None, None]:
    #     # TEST: This should cleanup data
    #     tmp_dir = tempfile.TemporaryDirectory()
    #     tmp_dirpath = Path(tmp_dir.name)
    #     try:
    #         yield tmp_dirpath
    #     except BaseException:  # Must provide an except clause in order to use 'else'
    #         raise
    #     else:  # elif not allowed
    #         if upload:
    #             self.upload_directory_contents(tmp_dirpath)
    #     finally:
    #         tmp_dir.cleanup()
