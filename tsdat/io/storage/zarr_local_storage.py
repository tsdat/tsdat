import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List

from pydantic import Field

from .file_system import FileSystem
from ..handlers import ZarrHandler

logger = logging.getLogger(__name__)


class ZarrLocalStorage(FileSystem):
    """---------------------------------------------------------------------------------
    Handles data storage and retrieval for zarr archives on a local filesystem.

    Zarr is a special format that writes chunked data to a number of files underneath
    a given directory. This distribution of data into chunks and distinct files makes
    zarr an extremely well-suited format for quickly storing and retrieving large
    quantities of data.

    Args:
        parameters (Parameters): File-system specific parameters, such as the root path
            to where the Zarr archives should be saved, or additional keyword arguments
            to specific functions used by the storage API. See the Parameters class for
            more details.

        handler (ZarrHandler): The ZarrHandler class that should be used to handle data
            I/O within the storage API.

    ---------------------------------------------------------------------------------"""

    class Parameters(FileSystem.Parameters):
        data_storage_path: Path = Path("data/{location_id}")
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
        """

        data_filename_template: str = "{datastream}.{extension}"
        """Template string to use for data filenames.
        
        Allows substitution of the following parameters using curly braces '{}':
        
        * ``ext``: the file extension from the storage data handler
        * ``datastream`` from the dataset's global attributes
        * ``location_id`` from the dataset's global attributes
        * ``data_level`` from the dataset's global attributes
        * Any other global attribute that has a string or integer data type.
        """

    parameters: Parameters = Field(default_factory=Parameters)  # type: ignore
    handler: ZarrHandler = Field(default_factory=ZarrHandler)

    def _filter_between_dates(self, filepaths: Iterable[Path],
                              start: datetime, end: datetime,
                              ) -> List[Path]:
        # Zarr filenames don't include dates. There should also only be one filepath
        # matching the data to fetch, so warn if otherwise
        zarr_files = sorted(filepaths)
        if len(zarr_files) > 1:
            logger.warning("More than zarr file found: %s", zarr_files)
        return zarr_files


# TODO:
#  HACK: Update forward refs to get around error I couldn't replicate with simpler code
#  "pydantic.errors.ConfigError: field "parameters" not yet prepared
#  so type is still a ForwardRef..."
ZarrLocalStorage.update_forward_refs(Parameters=ZarrLocalStorage.Parameters)
