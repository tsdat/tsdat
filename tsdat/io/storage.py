# TODO: Implement FileSystem
# TODO: Implement FileSystemS3

import logging
import os
import shutil
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime
from pydantic import BaseSettings, validator
from pathlib import Path
from typing import Any, Dict, List, Optional
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
logger = logging.getLogger(__name__)


class FileSystem(Storage):
    """------------------------------------------------------------------------------------
    Handles data storage and retrieval for file-based data formats. Formats that write to
    directories (such as zarr) are not supported by the FileSystem storage class.
    ------------------------------------------------------------------------------------"""

    # TODO: @clansing refactor to use a 'StorageFile' class for custom file naming
    # conventions. Until then, we will assume that we are using tsdat naming conventions
    # e.g., datastream = location.dataset_name[-qualifier][-temporal].data_level,
    # filename = datastream.YYYYMMDD.hhmmss.<extension>
    # filepath = <storage root>/location/datastream/filename

    class Parameters(BaseSettings):
        storage_root: Path = Path.cwd() / "storage" / "root"
        tmp_copy_symlinks: bool = True
        file_timespan: Optional[str] = None
        merge_fetched_data_kwargs: Dict[str, Any] = dict()

        @validator("storage_root")
        @classmethod
        def _ensure_storage_root_exists(cls, storage_root: Path) -> Path:
            if not storage_root.is_dir():
                logger.info("Creating storage root at: %s", storage_root.as_posix())
                storage_root.mkdir(parents=True)
            return storage_root

    parameters: Parameters = Parameters()
    handler: FileHandler

    def save_data(self, dataset: xr.Dataset):
        """------------------------------------------------------------------------------------
        Saves a dataset to the storage area. At a minimum, the dataset must have a 'datastream'
        global attribute and must have a 'time' variable with a np.datetime64-like data type.

        Args:
            dataset (xr.Dataset): The dataset to save.

        ------------------------------------------------------------------------------------"""
        # TODO: Use file_timespan to save the dataset as multiple files
        datastream = dataset.attrs["datastream"]
        filepath = self._get_dataset_filepath(dataset, datastream)
        filepath.parent.mkdir(exist_ok=True, parents=True)
        self.handler.writer.write(dataset, filepath)
        logger.info("Saved dataset to %s", filepath.as_posix())

    def fetch_data(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        """------------------------------------------------------------------------------------
        Fetches data for a given datastream between a specified time range.

        Note: this method is not smart; it searches for the appropriate data files using their
        filenames and does not filter within each data file.

        Args:
            start (datetime): The minimum datetime to fetch.
            end (datetime): The maximum datetime to fetch.
            datastream (str): The datastream id to search for.

        Returns:
            xr.Dataset: A dataset containing all the data in the storage area that spans the
            specified datetimes.

        ------------------------------------------------------------------------------------"""
        data_files = self._find_data(start, end, datastream)
        datasets = self._open_data_files(*data_files)
        return xr.merge(datasets, **self.parameters.merge_fetched_data_kwargs)  # type: ignore

    def save_ancillary_file(self, filepath: Path, datastream: str):
        """------------------------------------------------------------------------------------
        Saves an ancillary filepath to the datastream's ancillary storage area.

        Args:
            filepath (Path): The path to the ancillary file.
            datastream (str): The datastream that the file is related to.

        ------------------------------------------------------------------------------------"""
        ancillary_filepath = self._get_ancillary_filepath(filepath, datastream)
        ancillary_filepath.parent.mkdir(exist_ok=True, parents=True)
        saved_filepath = shutil.copy2(filepath, ancillary_filepath)
        logger.info("Saved ancillary file to: %s", saved_filepath)

    def _find_data(self, start: datetime, end: datetime, datastream: str) -> List[Path]:
        data_dirpath = self.parameters.storage_root / "data" / datastream
        filepaths = [data_dirpath / Path(file) for file in os.listdir(data_dirpath)]
        return self._filter_between_dates(filepaths, start, end)

    def _filter_between_dates(
        self, filepaths: List[Path], start: datetime, end: datetime
    ) -> List[Path]:
        # HACK: Currently can overshoot on both sides of the given range
        def __get_date_str(file: Path) -> str:
            name_components = file.name.split(".")
            date_components = name_components[3:5]
            return ".".join(date_components)

        start_date_str = start.strftime("%Y%m%d.%H%M%S")
        end_date_str = end.strftime("%Y%m%d.%H%M%S")

        valid_filepaths: List[Path] = []
        for filepath in filepaths:
            file_date_str = __get_date_str(filepath)
            if start_date_str <= file_date_str <= end_date_str:
                valid_filepaths.append(filepath)
        return valid_filepaths

    def _open_data_files(self, *filepaths: Path) -> List[xr.Dataset]:
        dataset_list: List[xr.Dataset] = []
        for filepath in filepaths:
            data = self.handler.reader.read(filepath.as_posix())
            if isinstance(data, dict):
                data = xr.merge(data.values())  # type: ignore
            dataset_list.append(data)
        return dataset_list

    def _get_dataset_filepath(self, dataset: xr.Dataset, datastream: str) -> Path:
        datastream_dir = self.parameters.storage_root / "data" / datastream
        start_datetime64: np.datetime64 = dataset.time.data[0]
        start_datetime = pd.Timestamp(start_datetime64).to_pydatetime()
        start_datetime_str = start_datetime.strftime("%Y%m%d.%H%M%S")
        ext = self.handler.writer.file_extension
        return datastream_dir / f"{datastream}.{start_datetime_str}.{ext}"

    def _get_ancillary_filepath(self, filepath: Path, datastream: str) -> Path:
        anc_datastream_dir = self.parameters.storage_root / "ancillary" / datastream
        return anc_datastream_dir / filepath.name


# class S3Storage(FileSystem):
#     def save_data(self, dataset: xr.Dataset):
#         # create tmp directories
#         for writer in self.writers:
#             writer.write(dataset, Path("some_tmp_file"), DatasetConfig())
#             self.upload_tmp_file("some tmp file")
