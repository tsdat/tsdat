# TODO: Implement FileSystem
# TODO: Implement FileSystemS3

from datetime import datetime
from pydantic import BaseSettings
import xarray as xr
from pathlib import Path
from typing import List, Optional
from .base import Storage
from .handlers import FileHandler


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
    # TODO: @clansing refactor to use a 'StorageFile' class for custom file naming
    # conventions.
    class Parameters(BaseSettings):
        storage_root: Path = Path.cwd() / "storage" / "root"
        tmp_copy_symlinks: bool = True
        file_timespan: Optional[str] = None

    parameters: Parameters = Parameters()
    handler: FileHandler

    def save_data(self, dataset: xr.Dataset):
        # create tmp dir
        # get filename for handler and put in tmp dir
        # call handler.write(dataset, tmp_file)
        # TODO
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

    # def get_filename(self, dataset: xr.Dataset, handler: FileHandler) -> str:
    #     ...

    # def get_filepath(self, dataset: xr.Dataset, handler: FileHandler) -> str:
    #     ...
    # def get_datastream_folder(self, dataset: xr.Dataset, handler: FileHandler) -> str:
    #     ...

    # def parse_filename(self, filepath: Path) -> "StorageFile":
    #     ...

    # class StorageFile(BaseModel):
    #     filepath: str
    #     basename: str
    #     datastream: str
    #     location: str
    #     begin_time: datetime
    #     end_time: datetime


# class S3Storage(FileSystem):
#     def save_data(self, dataset: xr.Dataset):
#         # create tmp directories
#         for writer in self.writers:
#             writer.write(dataset, Path("some_tmp_file"), DatasetConfig())
#             self.upload_tmp_file("some tmp file")
