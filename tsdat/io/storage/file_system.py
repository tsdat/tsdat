import shutil
import xarray as xr
from pathlib import Path
from pydantic import BaseSettings

from .storage import BaseStorage


class FileSystemParameters(BaseSettings):
    storage_root: Path = Path.cwd() / "storage" / "root"
    retain_input_files: bool = True
    tmp_copy_symlinks: bool = True


class FileSystem(BaseStorage):
    parameters: FileSystemParameters = FileSystemParameters()

    def save(self, dataset: xr.Dataset):
        # TODO: Must consider where data is written to (if file-based writers).
        self.registry.write(dataset=dataset)

    def upload_directory_contents(self, dirpath: Path):
        # TEST: This should copy from tmp to storage root like so:
        # <dirpath> = /tmp/randomlettersandnumbers
        # <storage_root> = ./storage/root (or something custom)
        # <dirpath>/folder/file1 -> <storage_root>/folder/file1
        shutil.copytree(
            src=dirpath,
            dst=self.parameters.storage_root,
            dirs_exist_ok=True,
            symlinks=self.parameters.tmp_copy_symlinks,
        )
