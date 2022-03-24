# TODO: Implement FileSystem
# TODO: Implement S3

import contextlib
import tempfile
from pydantic import BaseSettings
import xarray as xr
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Any, Generator
from tsdat.io.handlers.handlers import HandlerRegistry
from tsdat.utils import ParametrizedClass


class BaseStorage(ParametrizedClass, ABC):

    parameters: Any = {}
    registry: HandlerRegistry

    @abstractmethod
    def save_data(self, dataset: xr.Dataset):
        ...

    # @abstractmethod
    # def find_data(self, start: datetime, end: datetime, datastream: str):
    #     ...
    # @abstractmethod
    # def fetch_data(self, start: datetime, end: datetime, datastream: str):
    #     ...
    # @abstractmethod
    # def delete_data(self, start: datetime, end: datetime, datastream: str):
    #     ...

    @abstractmethod
    def save_ancillary_file(self, file: Path, datastream: str):
        ...

    @contextlib.contextmanager
    def uploadable_tmp_dir(self, datastream: str) -> Generator[Path, None, None]:
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dirpath = Path(tmp_dir.name)
        try:
            yield tmp_dirpath
        except BaseException:
            raise
        else:
            for path in tmp_dirpath.glob("**/*"):
                if path.is_file:
                    self.save_ancillary_file(file=path, datastream=datastream)
        finally:
            tmp_dir.cleanup()


class FileSystemParameters(BaseSettings):
    storage_root: Path = Path.cwd() / "storage" / "root"
    tmp_copy_symlinks: bool = True


class FileSystem(BaseStorage):
    parameters: FileSystemParameters = FileSystemParameters()

    def save(self, dataset: xr.Dataset):
        # TODO: Must consider where data is written to (if file-based writers).
        self.registry.write(dataset=dataset)
