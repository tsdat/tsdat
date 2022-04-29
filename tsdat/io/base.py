import tempfile
import contextlib
import xarray as xr
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Pattern, Union
from abc import ABC, abstractmethod
from ..utils import ParameterizedClass
from ..config.dataset import DatasetConfig


__all__ = [
    "DataConverter",
    "DataReader",
    "DataWriter",
    "FileWriter",
    "DataHandler",
    "FileHandler",
    "Retriever",
    "Storage",
]


class DataConverter(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for running data conversions on retrieved raw dataset.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def convert(
        self,
        dataset: xr.Dataset,
        dataset_config: DatasetConfig,
        variable_name: str,
        **kwargs: Any,
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Runs the data converter on the provided (retrieved) dataset.

        Args:
            dataset (xr.Dataset): The dataset to convert.
            dataset_config (DatasetConfig): The dataset configuration.
            variable_name (str): The name of the variable to convert.

        Returns:
            xr.Dataset: The converted dataset.

        -----------------------------------------------------------------------------"""
        ...


# TODO: VariableFinder
# TODO: DataTransformer


class DataReader(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for reading data from an input source.

    Args:
        regex (Pattern[str]): The regex pattern associated with the DataReader. If
        calling the DataReader from a tsdat pipeline, this pattern will be checked
        against each possible input key before the read() method is called.

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def read(self, input_key: str) -> Union[xr.Dataset, Dict[str, xr.Dataset]]:
        """-----------------------------------------------------------------------------
        Reads data given an input key.

        Uses the input key to open a resource and load data as a xr.Dataset object or as
        a mapping of strings to xr.Dataset objects.

        In most cases DataReaders will only need to return a single xr.Dataset, but
        occasionally some types of inputs necessitate that the data loaded from the
        input_key be returned as a mapping. For example, if the input_key is a path to a
        zip file containing multiple disparate datasets, then returning a mapping is
        appropriate.

        Args:
            input_key (str): An input key matching the DataReader's regex pattern that
                should be used to load data.

        Returns:
            Union[xr.Dataset, Dict[str, xr.Dataset]]: The raw data extracted from the
                provided input key.

        -----------------------------------------------------------------------------"""
        ...


class DataWriter(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for writing data to storage area(s).

    ---------------------------------------------------------------------------------"""

    @abstractmethod
    def write(self, dataset: xr.Dataset, **kwargs: Any) -> None:
        """-----------------------------------------------------------------------------
        Writes the dataset to the storage area.

        This method is typically called by the tsdat storage API, which will be
        responsible for providing any additional parameters required by subclasses of
        the tsdat.io.base.DataWriter class.

        Args:
            dataset (xr.Dataset): The dataset to save.

        -----------------------------------------------------------------------------"""
        ...


class FileWriter(DataWriter, ABC):
    """---------------------------------------------------------------------------------
    Base class for file-based DataWriters.

    Args:
        file_extension (str): The file extension that the FileHandler should be used
            for, e.g., ".nc", ".csv", ...

    ---------------------------------------------------------------------------------"""

    file_extension: str

    @abstractmethod
    def write(
        self, dataset: xr.Dataset, filepath: Optional[Path] = None, **kwargs: Any
    ) -> None:
        """-----------------------------------------------------------------------------
        Writes the dataset to the provided filepath.

        This method is typically called by the tsdat storage API, which will be
        responsible for providing the filepath, including the file extension.

        Args:
            dataset (xr.Dataset): The dataset to save.
            filepath (Optional[Path]): The path to the file to save.

        -----------------------------------------------------------------------------"""
        ...


class DataHandler(ParameterizedClass):
    """---------------------------------------------------------------------------------
    Groups a DataReader subclass and a DataWriter subclass together.

    This provides a unified approach to data I/O. DataHandlers are typically expected
    to be able to round-trip the data, i.e. the following psuedocode is generally true:

        `handler.read(handler.write(dataset))) == dataset`

    Args:
        reader (DataReader): The DataReader subclass responsible for reading input data.
        writer (FileWriter): The FileWriter subclass responsible for writing output
        data.

    ---------------------------------------------------------------------------------"""

    parameters: Any
    reader: DataReader
    writer: DataWriter


class FileHandler(DataHandler):
    """---------------------------------------------------------------------------------
    DataHandler specifically tailored to reading and writing files of a specific type.

    Args:
        reader (DataReader): The DataReader subclass responsible for reading input data.
        writer (FileWriter): The FileWriter subclass responsible for writing output
        data.

    ---------------------------------------------------------------------------------"""

    reader: DataReader
    writer: FileWriter


class Retriever(ParameterizedClass, ABC):
    """---------------------------------------------------------------------------------
    Base class for retrieving data used as input to tsdat pipelines.

    Args:
        readers (Dict[str, DataReader]): The mapping of readers that should be used to
            retrieve data given input_keys and optional keyword arguments provided by
            subclasses of Retriever.

    ---------------------------------------------------------------------------------"""

    readers: Dict[Pattern, Any]  # type: ignore
    """Mapping of readers that should be used to read data given input keys."""

    @abstractmethod
    def retrieve(
        self, input_keys: List[str], dataset_config: DatasetConfig, **kwargs: Any
    ) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Prepares the raw dataset mapping for use in downstream pipeline processes.

        This is done by consolidating the data into a single xr.Dataset object. The
        retrieved dataset may contain additional coords and data_vars that are not
        defined in the output dataset. Input data converters are applied as part of the
        preparation process.

        Args:
            input_keys (List[str]): The input keys the registered DataReaders should
                read from.
            dataset_config (DatasetConfig): The specification of the output dataset.

        Returns:
            xr.Dataset: The retrieved dataset.

        -----------------------------------------------------------------------------"""
        ...


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

    parameters: Any = {}
    """(Internal) parameters used by the storage API that can be set through
    configuration files, environment variables, or other means."""

    handler: DataHandler
    """Defines methods for reading and writing datasets from the storage area."""

    @abstractmethod
    def save_data(self, dataset: xr.Dataset):
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
    def fetch_data(self, start: datetime, end: datetime, datastream: str) -> xr.Dataset:
        """-----------------------------------------------------------------------------
        Fetches a dataset from the storage area.

        The timespan of the returned dataset is between the specified start and end
        times.

        Args:
            start (datetime): The start time bound.
            end (datetime): The end time bound.
            datastream (str): The name of the datastream to fetch.

        Returns:
            xr.Dataset: The fetched dataset.

        -----------------------------------------------------------------------------"""
        ...

    @abstractmethod
    def save_ancillary_file(self, filepath: Path, datastream: str):
        """-----------------------------------------------------------------------------
        Saves an ancillary file to the storage area for the specified datastream.

        Ancillary files are plots or other non-dataset metadata files.

        Args:
            filepath (Path): Where the file that should be saved is currently located.
            datastream (str): The datastream that the ancillary file is associated with.

        -----------------------------------------------------------------------------"""
        ...

    @contextlib.contextmanager
    def uploadable_dir(self, datastream: str) -> Generator[Path, None, None]:
        """-----------------------------------------------------------------------------
        Context manager that can be used to upload many ancillary files at once.

        This method yields the path to a temporary directory whose contents will be
        saved to the storage area using the save_ancillary_file method upon exiting the
        context manager.

        Args:
            datastream (str): The datastream associated with any files written to the
                uploadable directory.

        Yields:
            Generator[Path, None, None]: A temporary directory whose contents should be
                saved to the storage area.

        -----------------------------------------------------------------------------"""
        tmp_dir = tempfile.TemporaryDirectory()
        tmp_dirpath = Path(tmp_dir.name)
        try:
            yield tmp_dirpath
        except BaseException:
            raise
        else:
            for path in tmp_dirpath.glob("**/*"):
                if path.is_file():
                    self.save_ancillary_file(path, datastream)
        finally:
            tmp_dir.cleanup()
