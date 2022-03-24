# TODO: Implement FileSystem
# TODO: Implement S3

from abc import ABC
from datetime import datetime
from pydantic import BaseSettings
import xarray as xr
from pathlib import Path
from typing import Dict, List, Optional
from .base import Storage
from .writers import FileWriter


class FileSystemParameters(BaseSettings):
    storage_root: Path = Path.cwd() / "storage" / "root"
    tmp_copy_symlinks: bool = True
    file_timespan: Optional[str] = None

    # TODO: interval / split files apart by some timeframe (e.g., 1 day)
    #
    # Optional:
    # file_timespan: 1D
    #
    #
    # psuedocode: Soley for splitting up a file into multiple chunks. Searching for
    # previous + merging probably happens when you actually store the dataset, if that's
    # something we care about

    # start_time = 00:00:00 (midnight for the date of the first timestamp in the dataset)
    # first_interval = [start_time: start_time + file_time_interval]
    # start_time += file_time_interval
    # until start_time + file_time_interval >= timestamp of the last point of the dataset

class FileSystem(Storage):
    parameters: FileSystemParameters = FileSystemParameters()
    writers: Dict[str, FileWriter]

    def save_data(self, dataset: xr.Dataset):
        ...

    def find_data(self, start: datetime, end: datetime, datastream: str) -> List[Path]:
        ...

    def open_data_files(self, *filepaths: Path) -> List[xr.Dataset]:
        # TODO... how...?
        ...

    def fetch_data(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        data_files: List[Path] = self.find_data(start, end, datastream)
        datasets: List[xr.Dataset] = self.open_data_files(*data_files)
        return xr.merge(datasets)  # type: ignore

    def save_ancillary_file(self, filepath: Path, datastream: str):
        # TODO
        ...


# class S3Storage(FileSystem):
#     def save_data(self, dataset: xr.Dataset):
#         # create tmp directories
#         for writer in self.writers:
#             writer.write(dataset, Path("some_tmp_file"), DatasetConfig())
#             self.upload_tmp_file("some tmp file")
