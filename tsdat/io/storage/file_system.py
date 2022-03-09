from pathlib import Path
import xarray as xr
from pydantic import BaseSettings

from .storage import BaseStorage


class FileSystemParameters(BaseSettings):
    storage_root: Path = Path.cwd() / "storage" / "root"
    retain_input_files: bool = True


class FileSystem(BaseStorage):
    parameters: FileSystemParameters = FileSystemParameters()

    def save(self, dataset: xr.Dataset):
        self.registry.write(dataset=dataset)

    def upload_directory_contents(self, dirpath: Path):
        # Copy all files into self.storage_root, preserving input structure
        ...
