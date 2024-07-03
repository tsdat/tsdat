import contextlib
import tempfile
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Union,
)

import xarray as xr
from pydantic import BaseSettings, Field

from ...tstring import Template
from ...utils import (
    ParameterizedClass,
    datetime_substitutions,
    get_fields_from_dataset,
    get_fields_from_datastream,
)
from .data_handler import DataHandler


class Storage(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Abstract base class for the tsdat Storage API. Subclasses of Storage are used in
    pipelines to persist data and ancillary files (e.g., plots).

    Args:
        parameters (Any): Configuration parameters for the Storage API. The specific
            parameters that are allowed will be defined by subclasses of this base
            class.
        handler (DataHandler): The DataHandler responsible for handling both read and
            write operations needed by the storage API.

    ---------------------------------------------------------------------------------"""

    class Parameters(BaseSettings):
        storage_root: Path = Field(Path("storage/root"), env="TSDAT_STORAGE_ROOT")
        """The path on disk where at least ancillary files will be saved to. For
        file-based storage classes this is also the root path for data files. Defaults
        to the `storage/root` folder in the active working directory.
        
        NOTE: This parameter can also be set via the ``TSDAT_STORAGE_ROOT`` environment
        variable."""

        ancillary_storage_path: str = "ancillary/{location_id}/{datastream}"
        """The directory structure under storage_root where ancillary files are saved.

        Allows substitution of the following parameters using curly braces '{}':
        
        * ``extension``: the file extension (e.g., 'png', 'gif').
        * ``datastream`` from the related xr.Dataset object's global attributes.
        * ``location_id`` from the related xr.Dataset object's global attributes.
        * ``data_level`` from the related xr.Dataset object's global attributes.
        * ``year, month, day, hour, minute, second`` of the first timestamp in the data.
        * ``date_time``: the first timestamp in the file formatted as "YYYYMMDD.hhmmss".
        * The names of any other global attributes of the related xr.Dataset object.

        Defaults to ``ancillary/{location_id}/{datastream}``."""

        ancillary_filename_template: str = (
            "{datastream}.{yyyy}{mm}{dd}.{HH}{MM}{SS}.{title}.{extension}"
        )
        """Template string to use for ancillary filenames.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``title``: a provided label for the ancillary file or plot.
        * ``extension``: the file extension (e.g., 'png', 'gif').
        * ``datastream`` from the related xr.Dataset object's global attributes.
        * ``location_id`` from the related xr.Dataset object's global attributes.
        * ``data_level`` from the related xr.Dataset object's global attributes.
        * ``year, month, day, hour, minute, second`` of the first timestamp in the data.
        * ``date_time``: the first timestamp in the file formatted as "YYYYMMDD.hhmmss".
        * The names of any other global attributes of the related xr.Dataset object.
        
        At a minimum the template must include ``{date_time}``."""

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore
    """Parameters used by the storage API that can be set through configuration files,
    environment variables, or directly."""

    handler: DataHandler
    """Defines methods for reading and writing datasets from the storage area."""

    def last_modified(self, datastream: str) -> Union[datetime, None]:
        """Find the last modified time for any data in that datastream.

        Args:
            datastream (str): The datastream.

        Returns:
            datetime: The datetime of the last modification.
        """

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
        return []

    @abstractmethod
    def save_data(self, dataset: xr.Dataset, **kwargs: Any):
        """-----------------------------------------------------------------------------
        Saves the dataset to the storage area.

        Args:
            dataset (xr.Dataset): The dataset to save.

        -----------------------------------------------------------------------------"""
        ...

    # @abstractmethod
    # def delete_data(self, start: datetime, end: datetime, datastream: str):
    #     ...
    # @abstractmethod
    # def find_data(self, start: datetime, end: datetime, datastream: str):
    #     ...

    @abstractmethod
    def fetch_data(
        self,
        start: datetime,
        end: datetime,
        datastream: str,
        metadata_kwargs: Union[Dict[str, str], None] = None,
        **kwargs: Any,
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Fetches a dataset from the storage area.

        The timespan of the returned dataset is between the specified start and end
        times.

        Args:
            start (datetime): The start time bound.
            end (datetime): The end time bound.
            datastream (str): The name of the datastream to fetch.
            metadata_kwargs (dict[str, str], optional): Metadata substitutions to help
                resolve the data storage path. This is only required if the template
                data storage path includes any properties other than datastream or
                fields contained in the datastream. Defaults to None.

        Returns:
            xr.Dataset: The fetched dataset.

        -----------------------------------------------------------------------------"""
        ...

    def get_ancillary_filepath(
        self,
        title: str,
        root_dir: Path,
        extension: str = "png",
        dataset: Union[xr.Dataset, None] = None,
        datastream: Union[str, None] = None,
        start: Union[datetime, None] = None,
        **kwargs: str,
    ) -> Path:
        """Returns the filepath for the given datastream and title of an ancillary file
        to be created.

        This method is typically used in the plotting hook of pipelines to get the path
        to where the plot file should be saved. In this case, it is recommend to use
        this in conjunction with ``with self.storage.uploadable_dir() as tmp_dir`` and
        use ``root_dir=tmp_dir`` as an argument to this function.

        Example:

        ```python

        # in ``hook_plot_dataset(self, dataset: xr.Dataset)``
        with self.storage.uploadable_dir() as tmp_dir:
            fig, ax = plt.subplots()

            # plotting code ...

            plot_file = self.storage.get_ancillary_filepath(
                title="wind_speed",
                extension="png",
                root_dir=tmp_dir,
                dataset=dataset,
            )
            fig.savefig(plot_file)
            plt.close(fig)
        ```

        Args:
            title (str): The title of the ancillary file or plot. Should be lowercase
                and use `_` instead of spaces.
            extension (str): The file extension to be used. Defaults to "png".
            dataset (xr.Dataset | None, optional): The dataset relating to the ancillary
                file. If provided, this is used to populate defaults for the datastream,
                start datetime, and other substitutions used to fill out the storage
                path template. Values from these other fields, if present, will take
                precedence.
            datastream (str | None, optional): The datastream relating to the ancillary
                file to be saved. Defaults to ``dataset.attrs["datastream"]``.
            start (datetime | None, optional): The datetime relating to the ancillary
                file to be saved. Defaults to ``dataset.time[0]``.
            root_dir (Path | None, optional): The root directory. If using a temporary
                (uploadable) directory, it is recommended to use that as the root_dir.
                Defaults to None.
            mkdirs (bool, optional): True if directories should be created, False
                otherwise. Defaults to True.
            **kwargs (str): Extra kwargs to use as substitutions for the ancillary
                storage path or filename templates, which may require more parameters
                than those already specified as arguments here. Defaults to
                ``**dataset.attrs``.

        Returns:
            Path: The path to the ancillary file.
        """
        substitutions = self._get_substitutions(
            title=title,
            dataset=dataset,
            datastream=datastream,
            start=start,
            extension=extension,
            extra=kwargs,
        )
        filepath_template = (
            Template(self.parameters.ancillary_storage_path)
            / self.parameters.ancillary_filename_template
        )
        ancillary_path = Path(filepath_template.substitute(substitutions))
        ancillary_path = root_dir / ancillary_path
        ancillary_path.parent.mkdir(exist_ok=True, parents=True)
        return ancillary_path

    @abstractmethod
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
        ...

    @contextlib.contextmanager
    def uploadable_dir(self, **kwargs: Any) -> Generator[Path, None, None]:
        """Context manager that can be used to upload many ancillary files at once.

        This method yields the path to a temporary directory whose contents will be
        saved to the storage area using the save_ancillary_file method upon exiting the
        context manager.

        Example:

        ```python
        # in ``hook_plot_dataset(self, dataset: xr.Dataset)``
        with self.storage.uploadable_dir() as tmp_dir:
            fig, ax = plt.subplots()

            # plotting code ...

            plot_file = self.storage.get_ancillary_filepath(
                title="wind_speed",
                extension="png",
                root_dir=tmp_dir,
                dataset=dataset,
            )
            fig.savefig(plot_file)
            plt.close(fig)
        ```

        Args:
            kwargs (Any): Unused. Included for backwards compatibility.

        Yields:
            Path: A temporary directory where files can be saved.
        """
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dirpath = Path(tmp_dir.name)

        yield tmp_dirpath

        for path in tmp_dirpath.glob("**/*"):
            if path.is_file():
                # Users are expected to call self.get_ancillary_filename() with
                # root_dir=tmp_dir (yield value from this function) or save files to
                # tmp_dir / filename (using root_dir=None, the default, for
                # get_ancillary_filename()).
                #
                # With these assumptions, we can get the target filepath by replacing
                # tmp_dir with self.parameters.storage_root
                target = self.parameters.storage_root / path.relative_to(tmp_dirpath)
                self.save_ancillary_file(path, target_path=target)

        tmp_dir.cleanup()

    def _get_substitutions(
        self,
        datastream: str | None = None,
        start: datetime | None = None,
        time_range: tuple[datetime, datetime] | None = None,
        dataset: xr.Dataset | None = None,
        extra: dict[str, str] | None = None,
        extension: str | None = None,
        title: str | None = None,
    ) -> Dict[str, Callable[[], str] | str]:
        """Gets substitutions for file extension and datastream components."""
        sub: dict[str, Callable[[], str] | str] = {}

        if extension is not None:
            sub.update(ext=extension, extension=extension)

        if start:
            sub.update(datetime_substitutions(start))

        # Get substitutions for year/month/day
        if time_range is not None:
            start, end = time_range
            if start.year == end.year:
                sub["year"] = start.strftime("%Y")  # yyyy
                sub["yyyy"] = start.strftime("%Y")
                if start.month == end.month:
                    sub["month"] = start.strftime("%m")  # mm
                    sub["mm"] = start.strftime("%m")
                    if start.day == end.day:
                        sub["day"] = start.strftime("%d")  # dd
                        sub["dd"] = start.strftime("%d")

        if dataset is not None:
            sub.update(get_fields_from_dataset(dataset))

        if extra is not None:
            sub.update(extra)

        if datastream is not None:
            sub.update(
                datastream=datastream,
                **get_fields_from_datastream(datastream),
            )

        if title is not None:
            sub.update(title=title)

        return sub
