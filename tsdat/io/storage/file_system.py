import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

import xarray as xr
from pydantic import Field, validator

from tsdat.tstring import Template

from ...utils import (
    get_fields_from_dataset,
    get_fields_from_datastream,
    get_file_datetime_str,
)
from ..base import Storage
from ..handlers import FileHandler, NetCDFHandler

logger = logging.getLogger(__name__)


class FileSystem(Storage):
    """Handles data storage and retrieval for file-based data formats.

    Formats that write to directories (such as zarr) are not supported by the FileSystem
    storage class.

    Args:
        parameters (Parameters): File-system specific parameters, such as the root path
            to where files should be saved, or additional keyword arguments to specific
            functions used by the storage API. See the FileSystemStorage.Parameters
            class for more details.
        handler (FileHandler): The FileHandler class that should be used to handle data
            I/O within the storage API.
    """

    class Parameters(Storage.Parameters):
        data_storage_path: Path = Path("data/{location_id}/{datastream}")
        """The directory structure under storage_root where ancillary files are saved.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``storage_root``: the value from the ``storage_root`` parameter.
        * ``datastream``: the ``datastream`` as defined in the dataset config file.
        * ``location_id``: the ``location_id`` as defined in the dataset config file.
        * ``data_level``: the ``data_level`` as defined in the dataset config file.
        * ``year``: the year of the first timestamp in the file.
        * ``month``: the month of the first timestamp in the file.
        * ``day``: the day of the first timestamp in the file.
        * ``extension``: the file extension used by the output file writer.

        Defaults to ``data/{location_id}/{datastream}``.
        """

        data_filename_template: str = "{datastream}.{date_time}.{extension}"
        """Template string to use for data filenames.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``ext``: the file extension from the storage data handler
        * ``datastream`` from the dataset's global attributes
        * ``location_id`` from the dataset's global attributes
        * ``data_level`` from the dataset's global attributes
        * ``date_time``: the first timestamp in the file formatted as "YYYYMMDD.hhmmss"
        * Any other global attribute that has a string or integer data type.
        
        At a minimum the template must include ``{date_time}``.
        """

        merge_fetched_data_kwargs: Dict[str, Any] = dict()
        """Keyword arguments passed to xr.merge.
        
        Note that this will only be called if the DataReader returns a dictionary of
        xr.Datasets for a single input key."""

        @validator("storage_root")
        def _ensure_storage_root_exists(cls, storage_root: Path) -> Path:
            if not storage_root.is_dir():
                logger.info("Creating storage root at: %s", storage_root.as_posix())
                storage_root.mkdir(parents=True, exist_ok=True)
            return storage_root

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore
    handler: FileHandler = Field(default_factory=NetCDFHandler)  # type: ignore

    @property
    def data_filepath_template(self) -> Template:
        return Template(
            self.parameters.storage_root
            / self.parameters.data_storage_path
            / self.parameters.data_filename_template
        )

    @property
    def ancillary_filepath_template(self) -> Template:
        return Template(
            self.parameters.storage_root
            / self.parameters.data_storage_path
            / self.parameters.ancillary_filename_template
        )

    def last_modified(self, datastream: str) -> Union[datetime, None]:
        """Find the last modified time for any data in that datastream.

        Args:
            datastream (str): The datastream.

        Returns:
            datetime: The datetime of the last modification.
        """
        filepath_glob = self.data_filepath_template.substitute(
            self._get_substitutions(datastream=datastream),
            allow_missing=True,
            fill="*",
        )
        matches = self._get_matching_files(filepath_glob)
        last_modified = None
        for file in matches:
            mod_timestamp = file.lstat().st_mtime
            mod_time = datetime.fromtimestamp(mod_timestamp).astimezone(timezone.utc)
            last_modified = (
                mod_time if last_modified is None else max(last_modified, mod_time)
            )
        return last_modified

    def modified_since(
        self, datastream: str, last_modified: datetime
    ) -> List[datetime]:
        """Find the list of data dates that have been modified since the passed
        last modified date.

        Args:
            datastream (str): _description_
            last_modified (datetime): Should be equivalent to run date (the last time
                data were changed)

        Returns:
            List[datetime]: The data dates of files that were changed since the last
                modified date
        """
        filepath_glob = self.data_filepath_template.substitute(
            self._get_substitutions(datastream=datastream),
            allow_missing=True,
            fill="*",
        )
        matches = self._get_matching_files(filepath_glob)
        results: list[datetime] = []
        for file in matches:
            mod_timestamp = file.lstat().st_mtime
            mod_time = datetime.fromtimestamp(mod_timestamp).astimezone(timezone.utc)
            if mod_time > last_modified:
                data_timestamp = datetime.strptime(
                    get_file_datetime_str(file.name), "%Y%m%d.%H%M%S"
                )
                results.append(data_timestamp)
        return results

    def save_ancillary_file(
        self, filepath: Path, target_path: Union[Path, None] = None
    ):
        """Saves an ancillary filepath to the datastream's ancillary storage area.

        NOTE: In most cases this function should not be used directly. Instead, prefer
        using the ``self.uploadable_dir(*args, **kwargs)`` method.

        Args:
            filepath (Path): The path to the ancillary file. This is expected to have
                a standardized filename and should be saved under the ancillary storage
                path.
            target_path (str): The path to where the data should be saved.
        """
        target_path.parent.mkdir(exist_ok=True, parents=True)
        saved_filepath = shutil.copy2(filepath, target_path)
        logger.info("Saved ancillary file to: %s", saved_filepath)

    def save_data(self, dataset: xr.Dataset, **kwargs: Any):
        """-----------------------------------------------------------------------------
        Saves a dataset to the storage area.

        At a minimum, the dataset must have a 'datastream' global attribute and must
        have a 'time' variable with a np.datetime64-like data type.

        Args:
            dataset (xr.Dataset): The dataset to save.

        -----------------------------------------------------------------------------"""
        datastream = dataset.attrs["datastream"]
        substitutions = self._get_substitutions(datastream=datastream, dataset=dataset)
        filepath = Path(
            self.data_filepath_template.substitute(substitutions, allow_missing=False)
        )
        filepath.parent.mkdir(exist_ok=True, parents=True)
        self.handler.writer.write(dataset, filepath)
        logger.info("Saved %s dataset to %s", datastream, filepath.as_posix())

    def fetch_data(
        self,
        start: datetime,
        end: datetime,
        datastream: str,
        metadata_kwargs: Union[Dict[str, str], None] = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Fetches data for a given datastream between a specified time range.

        Args:
            start (datetime): The minimum datetime to fetch.
            end (datetime): The maximum datetime to fetch.
            datastream (str): The datastream id to search for.
            metadata_kwargs (dict[str, str], optional): Metadata substitutions to help
                resolve the data storage path. This is only required if the template
                data storage path includes any properties other than datastream or
                fields contained in the datastream. Defaults to None.

        Returns:
            xr.Dataset: A dataset containing all the data in the storage area that spans
            the specified datetimes.

        -----------------------------------------------------------------------------"""
        data_files = self._find_data(
            start, end, datastream, metadata_kwargs=metadata_kwargs
        )
        datasets = self._open_data_files(*data_files)
        dataset = xr.merge(datasets, **self.parameters.merge_fetched_data_kwargs)  # type: ignore
        if not dataset:
            logger.warning(
                "No data found for %s in range %s - %s", datastream, start, end
            )
            return dataset  # empty
        return dataset.sel(time=slice(start, end))

    def _find_data(
        self,
        start: datetime,
        end: datetime,
        datastream: str,
        metadata_kwargs: Dict[str, str] | None = None,
        **kwargs: Any,
    ) -> List[Path]:
        substitutions = self._get_substitutions(
            datastream=datastream, time_range=(start, end), extra=metadata_kwargs
        )
        filepath_glob = self.data_filepath_template.substitute(
            substitutions, allow_missing=True, fill="*"
        )
        matches = self._get_matching_files(filepath_glob)
        return self._filter_between_dates(matches, start, end)

    def _get_matching_files(self, filepath_glob: str) -> list[Path]:
        assert (
            "*" in filepath_glob  # need some regex remaining to match with
        ), "Naming scheme must distinguish between files within the same datastream"
        path_components = Path(filepath_glob).parts
        for i, path_component in enumerate(path_components):
            if "*" in path_component:
                break
        prefix, suffix = Path(*path_components[:i]), str(Path(*path_components[i:]))  # type: ignore
        matches = list(prefix.glob(suffix))
        return matches

    @staticmethod
    def _filter_between_dates(
        filepaths: Iterable[Path], start: datetime, end: datetime
    ) -> List[Path]:
        start_date_str = start.strftime("%Y%m%d.%H%M%S")
        end_date_str = end.strftime("%Y%m%d.%H%M%S")

        valid_filepaths: List[Path] = []
        for filepath in filepaths:
            file_date_str = get_file_datetime_str(filepath)
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

    def _get_substitutions(
        self,
        datastream: str | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        dataset: xr.Dataset | None = None,
        extra: dict[str, str] | None = None,
    ) -> Dict[str, str]:
        """Gets substitutions for file extension and datastream components."""
        extension = self.handler.extension or self.handler.writer.file_extension

        sub = dict(ext=extension, extension=extension)

        if datastream is not None:
            sub.update(
                datastream=datastream,
                **get_fields_from_datastream(datastream),
            )

        # Get substitutions for year/month/day
        if time_range is not None:
            start, end = time_range
            if start.year == end.year:
                sub["year"] = start.strftime("%Y")  # yyyy
                if start.month == end.month:
                    sub["month"] = start.strftime("%m")  # mm
                    if start.day == end.day:
                        sub["day"] = start.strftime("%d")  # dd

        if extra is not None:
            sub.update(extra)

        if dataset is not None:
            sub.update(get_fields_from_dataset(dataset))

        return sub


# TODO:
#  HACK: Update forward refs to get around error I couldn't replicate with simpler code
#  "pydantic.errors.ConfigError: field "parameters" not yet prepared
#  so type is still a ForwardRef..."
FileSystem.update_forward_refs(Parameters=FileSystem.Parameters)
